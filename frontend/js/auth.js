/*
 * Nexdata frontend auth helper (SPEC_127).
 *
 * Include before any page script that calls the API:
 *     <script src="/js/auth.js"></script>
 *
 * - Wraps window.fetch: requests to /api/... or /graphql on this origin (or
 *   the API's own localhost:8001 origin) get `Authorization: Bearer <token>`
 *   from localStorage['nexdata_token'] (the key the console login stores).
 * - On a 401 from a protected route, calls window.nexdataOnUnauthorized if the
 *   page defines it (the console shows its login overlay), otherwise sends
 *   the browser to the console login and back here afterwards.
 * - NexdataAuth.streamUrl(path) returns an EventSource URL carrying a
 *   short-lived ?stream_token= (EventSource cannot send headers).
 */
(function () {
  'use strict';

  var TOKEN_KEY = 'nexdata_token';
  var USER_KEY = 'nexdata_user';
  var APP_AUDIENCE = 'nexdata-app';
  var API_PORT_ORIGINS = ['http://localhost:8001', 'http://127.0.0.1:8001'];
  var LOGIN_PAGE = '/index.html';

  function storageGet(key) {
    try { return window.localStorage.getItem(key); } catch (e) { return null; }
  }
  function storageSet(key, value) {
    try { window.localStorage.setItem(key, value); } catch (e) { /* private mode */ }
  }
  function storageRemove(key) {
    try { window.localStorage.removeItem(key); } catch (e) { /* private mode */ }
  }

  function decodePayload(token) {
    try {
      var part = token.split('.')[1];
      var json = atob(part.replace(/-/g, '+').replace(/_/g, '/'));
      return JSON.parse(json);
    } catch (e) {
      return null;
    }
  }

  /** The stored token, but only if it is an unexpired platform token. */
  function getToken() {
    var token = storageGet(TOKEN_KEY);
    if (!token) return null;
    var payload = decodePayload(token);
    if (!payload) return null;
    var aud = payload.aud;
    var audOk = Array.isArray(aud) ? aud.indexOf(APP_AUDIENCE) !== -1 : aud === APP_AUDIENCE;
    if (!audOk) return null;
    if (payload.exp && payload.exp * 1000 < Date.now()) return null;
    return token;
  }

  function setSession(token, user) {
    storageSet(TOKEN_KEY, token);
    if (user) storageSet(USER_KEY, JSON.stringify(user));
  }

  function clearSession() {
    storageRemove(TOKEN_KEY);
    storageRemove(USER_KEY);
  }

  function resolve(url) {
    try { return new URL(url, window.location.href); } catch (e) { return null; }
  }

  function isApiUrl(url) {
    var u = resolve(url);
    if (!u) return false;
    var sameOrigin = u.origin === window.location.origin;
    var apiOrigin = API_PORT_ORIGINS.indexOf(u.origin) !== -1;
    if (!sameOrigin && !apiOrigin) return false;
    return u.pathname.indexOf('/api/') === 0 || u.pathname.indexOf('/graphql') === 0;
  }

  function isAuthEndpoint(url) {
    var u = resolve(url);
    return !!u && u.pathname.indexOf('/api/v1/auth/') === 0;
  }

  var redirecting = false;
  function handleUnauthorized() {
    if (typeof window.nexdataOnUnauthorized === 'function') {
      window.nexdataOnUnauthorized();
      return;
    }
    if (redirecting) return;
    redirecting = true;
    clearSession();
    var here = window.location.pathname + window.location.search;
    window.location.href = LOGIN_PAGE + '?next=' + encodeURIComponent(here);
  }

  var nativeFetch = window.fetch.bind(window);

  window.fetch = function (input, init) {
    var url = typeof input === 'string' ? input : (input && input.url) || '';
    if (!isApiUrl(url)) return nativeFetch(input, init);

    var token = getToken();
    var opts = init ? Object.assign({}, init) : {};
    if (token) {
      var headers = new Headers(opts.headers || (input && input.headers) || {});
      if (!headers.has('Authorization')) headers.set('Authorization', 'Bearer ' + token);
      opts.headers = headers;
    }
    return nativeFetch(input, opts).then(function (resp) {
      if (resp.status === 401 && !isAuthEndpoint(url)) handleUnauthorized();
      return resp;
    });
  };

  /**
   * EventSource URL for an admin SSE route. Fetches a 5-minute stream token
   * when signed in; with no token (REQUIRE_AUTH=false local dev) returns the
   * plain path. Call it again for every (re)connect.
   */
  function streamUrl(path) {
    if (!getToken()) return Promise.resolve(path);
    return window.fetch('/api/v1/auth/stream-token', { method: 'POST' })
      .then(function (resp) { return resp.ok ? resp.json() : null; })
      .then(function (body) {
        if (!body || !body.stream_token) return path;
        var sep = path.indexOf('?') === -1 ? '?' : '&';
        return path + sep + 'stream_token=' + encodeURIComponent(body.stream_token);
      })
      .catch(function () { return path; });
  }

  /**
   * Fetch an API URL with the bearer token and return an object URL for the
   * response body. Navigations (<a href>, iframe.src, window.open) cannot
   * send headers, so downloads and report previews go through this.
   */
  function objectUrl(url) {
    return window.fetch(url).then(function (resp) {
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      return resp.blob();
    }).then(function (blob) {
      var obj = URL.createObjectURL(blob);
      setTimeout(function () { URL.revokeObjectURL(obj); }, 5 * 60 * 1000);
      return obj;
    });
  }

  /** window.open for an API URL. Opens the window synchronously (popup
   *  blockers) and points it at the authenticated blob once it arrives. */
  function openApiUrl(url, target) {
    var win = target && target !== '_blank' ? null : window.open('', '_blank');
    return objectUrl(url).then(function (obj) {
      if (win) win.location.href = obj;
      else window.location.href = obj;
    }).catch(function (e) {
      if (win) win.close();
      throw e;
    });
  }

  // Plain <a href="/api/..."> links (report downloads) become authenticated
  // fetches when a token is present.
  document.addEventListener('click', function (e) {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey) return;
    var a = e.target && e.target.closest ? e.target.closest('a[href]') : null;
    if (!a || !isApiUrl(a.href) || !getToken()) return;
    e.preventDefault();
    openApiUrl(a.href, a.getAttribute('target') || '_self');
  }, true);

  window.NexdataAuth = {
    getToken: getToken,
    setSession: setSession,
    clearSession: clearSession,
    streamUrl: streamUrl,
    objectUrl: objectUrl,
    openApiUrl: openApiUrl,
    decodePayload: decodePayload,
    isApiUrl: isApiUrl,
  };
})();
