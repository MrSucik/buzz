import 'dart:async';
import 'dart:isolate';

import 'package:buzz/features/channels/channels_provider.dart';
import 'package:buzz/shared/community/community.dart';
import 'package:buzz/shared/community/community_provider.dart';
import 'package:buzz/shared/profile/user_cache_provider.dart';
import 'package:buzz/shared/push/push_presentation_cache.dart';
import 'package:buzz/shared/push/push_presentation_export_recovery.dart';
import 'package:buzz/shared/relay/relay.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:nostr/nostr.dart' as nostr;

const _bridge = MethodChannel('buzz/push');
const _secret =
    '0000000000000000000000000000000000000000000000000000000000000001';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  final messenger =
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
  setUp(() {
    debugDefaultTargetPlatformOverride = TargetPlatform.iOS;
    pushPresentationExportError.value = null;
  });
  tearDown(() {
    debugDefaultTargetPlatformOverride = null;
    pushPresentationExportError.value = null;
    messenger.setMockMethodCallHandler(_bridge, null);
  });

  for (final profiles in [true, false]) {
    final section = profiles ? 'profiles' : 'channels';
    test(
      '$section producer recovers its saturated detached snapshot',
      () async {
        final blocked = Completer<void>();
        final entered = Completer<void>();
        final delivered = Completer<Map<dynamic, dynamic>>();
        messenger.setMockMethodCallHandler(_bridge, (call) async {
          final args = call.arguments as Map<dynamic, dynamic>;
          if (args['communityId'] == 'blocker' && !entered.isCompleted) {
            entered.complete();
            await blocked.future;
          }
          if (args['communityId'] == 'original' && !delivered.isCompleted) {
            delivered.complete(args);
          }
          return null;
        });
        final filler = _signed(0);
        final queued = [
          cacheBuzzPushProfileEvents('blocker', [filler]),
        ];
        await entered.future;
        for (var i = 1; i < 8; i++) {
          queued.add(cacheBuzzPushProfileEvents('queued-$i', [filler]));
        }
        final event = _signed(profiles ? 0 : 39000);
        final membership = _signed(39002);
        var communityID = 'original';
        final container = _container(
          _Session(event, membership),
          communityID: () => communityID,
        );
        addTearDown(container.dispose);
        try {
          await container.read(activeCommunityProvider.future);
          if (profiles) {
            expect(
              await container.read(userCacheProvider.notifier).preload([
                event.pubkey,
              ]),
              isTrue,
            );
          } else {
            await container.read(channelsProvider.future);
          }
          expect(delivered.isCompleted, isFalse);
          communityID = 'replacement';
          container.invalidate(activeCommunityProvider);
          await container.read(activeCommunityProvider.future);
        } finally {
          blocked.complete();
          await Future.wait(queued);
        }
        final payload = await delivered.future.timeout(
          const Duration(seconds: 5),
        );
        expect(payload['section'], section);
        expect(payload['communityId'], 'original');
        expect(payload[profiles ? 'events' : 'metadataEvents'], [
          event.toJson(),
        ]);
        if (!profiles) {
          expect(payload['membershipEvents'], [membership.toJson()]);
        }
        expect(pushPresentationExportError.value, isNull);
      },
    );

    test(
      '$section producer handles detached worker submission failure',
      () async {
        final port = ReceivePort();
        addTearDown(port.close);
        final event = _Unsendable(_signed(profiles ? 0 : 39000), port);
        final failure = Completer<void>();
        void onError() {
          if (pushPresentationExportError.value != null &&
              !failure.isCompleted) {
            failure.complete();
          }
        }

        pushPresentationExportError.addListener(onError);
        addTearDown(() => pushPresentationExportError.removeListener(onError));
        var nativeCalls = 0;
        messenger.setMockMethodCallHandler(_bridge, (_) async {
          nativeCalls++;
          return null;
        });
        final container = _container(_Session(event, _signed(39002)));
        addTearDown(container.dispose);
        await container.read(activeCommunityProvider.future);
        if (profiles) {
          await container.read(userCacheProvider.notifier).preload([
            event.pubkey,
          ]);
        } else {
          await container.read(channelsProvider.future);
        }
        await failure.future.timeout(const Duration(seconds: 5));
        expect(nativeCalls, 0);
        final terminalError = pushPresentationExportError.value;
        await cacheBuzzPushProfileEvents('unrelated-success', [_signed(0)]);
        expect(nativeCalls, 1);
        expect(pushPresentationExportError.value, terminalError);
      },
    );
  }

  test(
    'one recovery slot, bounded retries, and reusable slot after exhaustion',
    () {
      fakeAsync((clock) {
        final recovery = PushPresentationExportRecovery();
        var attempts = 0;
        Future<void> full() async {
          attempts++;
          throw PushPresentationExportQueueFull();
        }

        bool? result;
        recovery.export(full).then((value) => result = value);
        clock.flushMicrotasks();
        bool? excess;
        recovery.export(full).then((value) => excess = value);
        clock.flushMicrotasks();
        expect(excess, isFalse);
        expect(attempts, 2);
        clock.elapse(const Duration(seconds: 8));
        expect(result, isFalse);
        expect(
          attempts,
          7,
        ); // First attempt, rejected second operation, five retries.
        final terminal = pushPresentationExportError.value;
        expect(terminal, isNotNull);
        clock.elapse(const Duration(days: 1));
        expect(attempts, 7);
        bool? recovered;
        recovery.export(() async {}).then((value) => recovered = value);
        clock.flushMicrotasks();
        expect(recovered, isTrue);
        expect(pushPresentationExportError.value, terminal);
      });
    },
  );
}

ProviderContainer _container(
  _Session session, {
  String Function()? communityID,
}) => ProviderContainer(
  retry: (_, _) => null,
  overrides: [
    relaySessionProvider.overrideWith(() => session),
    appLifecycleProvider.overrideWith(_Lifecycle.new),
    myPubkeyProvider.overrideWith((ref) => 'me'),
    activeCommunityProvider.overrideWith(
      (ref) async => Community(
        id: communityID?.call() ?? 'original',
        name: 'Synthetic',
        relayUrl: 'https://example.invalid',
        addedAt: DateTime(2026),
      ),
    ),
  ],
);

class _Session extends RelaySessionNotifier {
  _Session(this.event, this.membership);
  final NostrEvent event;
  final NostrEvent membership;
  @override
  SessionState build() => const SessionState(status: SessionStatus.connected);
  @override
  Future<List<NostrEvent>> fetchHistory(
    NostrFilter filter, {
    Duration timeout = const Duration(seconds: 8),
  }) async => filter.kinds.contains(event.kind)
      ? [event]
      : filter.kinds.contains(39002)
      ? [membership]
      : [];
  @override
  Future<List<NostrEvent>> queryRelay(
    List<NostrFilter> filters, {
    Duration timeout = const Duration(seconds: 8),
  }) async => [
    for (final filter in filters)
      ...await fetchHistory(filter, timeout: timeout),
  ];
  @override
  Future<void Function()> subscribe(
    NostrFilter filter,
    void Function(NostrEvent) onEvent, {
    void Function(String)? onClosed,
  }) async => () {};
}

class _Lifecycle extends AppLifecycleNotifier {
  @override
  AppLifecycleState build() => AppLifecycleState.resumed;
}

NostrEvent _signed(int kind) => NostrEvent.fromJson(
  nostr.Event.from(
    kind: kind,
    createdAt: 100,
    content: kind == 0 ? '{"name":"Synthetic"}' : '',
    tags: kind == 0
        ? []
        : [
            ['d', 'channel'],
            ['name', 'Synthetic'],
            if (kind == 39002) ['p', 'me'],
          ],
    secretKey: _secret,
  ).toMap(),
);

class _Unsendable extends NostrEvent {
  final ReceivePort port;
  _Unsendable(NostrEvent event, this.port)
    : super(
        id: event.id,
        pubkey: event.pubkey,
        createdAt: event.createdAt,
        kind: event.kind,
        tags: event.tags,
        content: event.content,
        sig: event.sig,
      );
}
