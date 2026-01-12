// Minimal runtime-safe polyfills for EventTarget, AbortController, ReadableStream, TransformStream
declare const globalThis: any;

if (typeof globalThis.EventTarget === "undefined") {
  class SimpleEventTarget {
    private _listeners: Record<string, Function[]> = {};
    addEventListener(type: string, cb: Function) {
      if (!this._listeners[type]) this._listeners[type] = [];
      this._listeners[type].push(cb);
    }
    removeEventListener(type: string, cb: Function) {
      if (!this._listeners[type]) return;
      this._listeners[type] = this._listeners[type].filter((f) => f !== cb);
    }
    dispatchEvent(event: any) {
      const list = this._listeners[event?.type] || [];
      for (const fn of list) {
        try { fn.call(this, event); } catch {}
      }
      return true;
    }
  }
  globalThis.EventTarget = SimpleEventTarget;
}

if (typeof globalThis.AbortController === "undefined") {
  class SimpleAbortSignal extends (globalThis.EventTarget || class {}) {
    aborted = false;
  }
  class SimpleAbortController {
    signal: any;
    constructor() {
      this.signal = new SimpleAbortSignal();
    }
    abort() {
      this.signal.aborted = true;
      try { this.signal.dispatchEvent({ type: "abort" }); } catch {}
    }
  }
  globalThis.AbortController = SimpleAbortController;
  globalThis.AbortSignal = SimpleAbortSignal;
}

if (typeof globalThis.ReadableStream === "undefined") {
  globalThis.ReadableStream = class {
    constructor() {}
  };
}

if (typeof globalThis.TransformStream === "undefined") {
  globalThis.TransformStream = class {
    constructor() {}
  };
}
