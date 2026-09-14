import 'package:flutter/foundation.dart';

import 'push_presentation_cache.dart';

/// Latest terminal export failure, retained across unrelated successful writes.
///
/// Unlike [pushPresentationCacheError], this records an export that could not
/// reach native storage. Retries are bounded and do not guarantee delivery.
final pushPresentationExportError = ValueNotifier<String?>(null);

/// Handles detached exports with one retained retry snapshot per producer.
///
/// Queue saturation retries the exact operation five times over 7.75 seconds.
/// A second saturated operation while recovery is occupied, exhausted retries,
/// or a worker failure returns false and records a terminal error. No extra
/// queue or retry loop is created, and unverified candidates are never merged.
class PushPresentationExportRecovery {
  bool _retrying = false;

  /// Exports once, recovering transient saturation without detached errors.
  Future<bool> export(Future<void> Function() operation) async {
    try {
      await operation();
      return true;
    } on PushPresentationExportQueueFull catch (error, stack) {
      if (_retrying) return _failed(error, stack);
    } catch (error, stack) {
      return _failed(error, stack);
    }

    _retrying = true;
    try {
      for (final milliseconds in [250, 500, 1000, 2000, 4000]) {
        await Future<void>.delayed(Duration(milliseconds: milliseconds));
        try {
          await operation();
          return true;
        } on PushPresentationExportQueueFull catch (error, stack) {
          if (milliseconds == 4000) return _failed(error, stack);
        } catch (error, stack) {
          return _failed(error, stack);
        }
      }
      return false;
    } finally {
      _retrying = false;
    }
  }

  bool _failed(Object error, StackTrace stack) {
    pushPresentationExportError.value = error.toString();
    debugPrint('Push presentation export could not be delivered: $error');
    debugPrintStack(stackTrace: stack);
    return false;
  }
}
