const CONFIG = {
  fetchTimeoutMs: 15000,
  maxRetries: 2,
  baseBackoffMs: 250,
  cacheTtlMs: 120000,
  thumbnailSize: 512,
  clientVersion: "1.20240801.01.00",
  debugRawJsonHead: 1200,
  maxResults: 12,
  allowlistHosts: []
};

function L(level, ...args) {
  try { if (typeof log !== "undefined" && typeof log[level] === "function") log[level](...args); } catch {}
}

function now() { return Date.now(); }

const _cache = new Map();
function cacheGet(k) {
  const e = _cache.get(k);
  if (!e) return null;
  if (now() - e.t > CONFIG.cacheTtlMs) { _cache.delete(k); return null; }
  return e.v;
}
function cacheSet(k, v) { _cache.set(k, { v, t: now() }); }

const _inflight = new Map();
function dedupFetch(key, fn) {
  if (_inflight.has(key)) return _inflight.get(key);
  const p = fn().finally(() => { _inflight.delete(key); });
  _inflight.set(key, p);
  return p;
}

async function safeFetch(url, opts) {
  opts = opts || {};
  for (let i = 0; i <= CONFIG.maxRetries; i++) {
    var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    var local = Object.assign({}, opts);
    if (controller) local.signal = controller.signal;
    var to;
    try {
      if (controller) to = setTimeout(function(){ controller.abort(); }, CONFIG.fetchTimeoutMs);
      var res = await fetch(url, local);
      if (to) clearTimeout(to);
      if (!res) throw new Error("no_response");
      if (res.status === 429 || res.status === 503) {
        var e = new Error("rate_limited"); e.retryable = true; e.status = res.status; throw e;
      }
      return res;
    } catch (err) {
      if (to) clearTimeout(to);
      var retryable = err && (err.retryable || err.name === "AbortError" || /Failed to fetch|NetworkError/.test(String(err.message)));
      if (!retryable || i === CONFIG.maxRetries) { L("error", "safeFetch final", String(err)); throw err; }
      var back = CONFIG.baseBackoffMs * Math.pow(2, i) + Math.floor(Math.random() * 100);
      L("warn", "safeFetch retry", { url: url, attempt: i + 1, back: back });
      await new Promise(function(r){ setTimeout(r, back); });
    }
  }
  throw new Error("safeFetch_failed");
}

function isString(v) { return typeof v === "string"; }

function normalizeUrl(u) {
  if (!isString(u)) return null;
  var s = u.trim();
  if (!s) return null;
  if (!/^https?:\/\//i.test(s)) return null;
  try {
    var parsed = new URL(s);
    if (Array.isArray(CONFIG.allowlistHosts) && CONFIG.allowlistHosts.length > 0) {
      if (CONFIG.allowlistHosts.indexOf(parsed.hostname) === -1 && !/^https?:\/\//i.test(parsed.protocol + "//")) {
        // no-op, primary check already ensures http(s)
      }
    }
    return parsed.toString();
  } catch (e) {
    return null;
  }
}

function isAbsoluteHttpUrl(u) { return isString(u) && /^https?:\/\//i.test(u.trim()); }

function makeSquareThumb(url) {
  var u = normalizeUrl(url);
  if (!u) return null;
  try {
    var replaced = u.replace(/=w\d+-h\d+/g, "=w" + CONFIG.thumbnailSize + "-h" + CONFIG.thumbnailSize)
                    .replace(/\/s\d+-c/g, "/s" + CONFIG.thumbnailSize + "-c");
    return normalizeUrl(replaced);
  } catch (e) {
    return null;
  }
}

function parseDurationText(t) {
  if (!t) return 0;
  var m = String(t).match(/(\d{1,2}:)?\d{1,2}:\d{2}|\d{1,2}:\d{2}/);
  if (!m) return 0;
  var parts = m[0].split(":").map(function(x){ return parseInt(x, 10); });
  if (parts.some(function(p){ return isNaN(p); })) return 0;
  var s = 0;
  for (var i = 0; i < parts.length; i++) s = s * 60 + parts[i];
  return s;
}

function extractVideoIdFromEndpoint(ep) {
  try {
    if (!ep) return null;
    if (ep.watchEndpoint && ep.watchEndpoint.videoId) return ep.watchEndpoint.videoId;
    if (ep.commandMetadata && ep.commandMetadata.webCommandMetadata && ep.commandMetadata.webCommandMetadata.url) {
      var m = String(ep.commandMetadata.webCommandMetadata.url).match(/v=([^&]+)/);
      if (m) return m[1];
    }
    if (ep.browseEndpoint && ep.browseEndpoint.browseId) return ep.browseEndpoint.browseId;
    return null;
  } catch (e) {
    return null;
  }
}

function normalizeCandidate(info) {
  if (!info) return null;
  if (info.musicResponsiveListItemRenderer) return info.musicResponsiveListItemRenderer;
  if (info.musicTwoRowItemRenderer) return info.musicTwoRowItemRenderer;
  if (info.musicCardRenderer) return info.musicCardRenderer;
  if (info.videoRenderer) return info.videoRenderer;
  if (info.richItemRenderer && info.richItemRenderer.content) return info.richItemRenderer.content;
  if (info.playlistPanelVideoRenderer) return info.playlistPanelVideoRenderer;
  return info;
}

function pickLastThumbnailUrl(thumbnailObj) {
  try {
    if (!thumbnailObj) return null;
    if (Array.isArray(thumbnailObj)) {
      if (thumbnailObj.length === 0) return null;
      var last = thumbnailObj[thumbnailObj.length - 1];
      return last && last.url ? last.url : null;
    }
    if (thumbnailObj.thumbnails && Array.isArray(thumbnailObj.thumbnails) && thumbnailObj.thumbnails.length) {
      var l = thumbnailObj.thumbnails[thumbnailObj.thumbnails.length - 1];
      return l && l.url ? l.url : null;
    }
    return null;
  } catch (e) {
    return null;
  }
}

function parseItemExtended(info) {
  try {
    if (!info) return null;
    var c = normalizeCandidate(info);
    if (!c) return null;
    var title = null;
    if (c.title && c.title.runs && c.title.runs[0] && c.title.runs[0].text) title = c.title.runs[0].text;
    if (!title && c.title && c.title.simpleText) title = c.title.simpleText;
    if (!title && c.titleText && c.titleText.runs && c.titleText.runs[0] && c.titleText.runs[0].text) title = c.titleText.runs[0].text;
    if (!title && c.name && c.name.simpleText) title = c.name.simpleText;
    if (!title && c.video && c.video.title) title = c.video.title;
    if (!title && c.header && c.header.title && c.header.title.runs) title = c.header.title.runs.map(function(r){return r.text;}).join(" ");
    var artist = "";
    if (c.subtitle && c.subtitle.runs) artist = c.subtitle.runs.map(function(r){ return r.text; }).join(" ");
    if (!artist && c.longBylineText && c.longBylineText.runs) artist = c.longBylineText.runs.map(function(r){ return r.text; }).join(" ");
    if (!artist && c.ownerText && c.ownerText.runs) artist = c.ownerText.runs.map(function(r){ return r.text; }).join(" ");
    var videoId = null;
    if (c.playlistItemData && c.playlistItemData.videoId) videoId = c.playlistItemData.videoId;
    if (!videoId && c.videoId) videoId = c.videoId;
    if (!videoId && c.navigationEndpoint) videoId = extractVideoIdFromEndpoint(c.navigationEndpoint);
    if (!videoId && c.thumbnail && c.thumbnail.musicThumbnailRenderer && c.thumbnail.musicThumbnailRenderer.navigationEndpoint) videoId = extractVideoIdFromEndpoint(c.thumbnail.musicThumbnailRenderer.navigationEndpoint);
    if (!videoId && c.video && c.video.videoId) videoId = c.video.videoId;
    if (!title || !videoId) return null;
    var durationText = "";
    if (c.lengthText && c.lengthText.simpleText) durationText = c.lengthText.simpleText;
    if (!durationText && c.thumbnailOverlays && c.thumbnailOverlays[0] && c.thumbnailOverlays[0].thumbnailOverlayTimeStatusRenderer && c.thumbnailOverlays[0].thumbnailOverlayTimeStatusRenderer.text && c.thumbnailOverlays[0].thumbnailOverlayTimeStatusRenderer.text.simpleText) {
      durationText = c.thumbnailOverlays[0].thumbnailOverlayTimeStatusRenderer.text.simpleText;
    }
    if (!durationText && c.badges && c.badges.length) {
      for (var bi = 0; bi < c.badges.length; bi++) {
        var b = c.badges[bi];
        if (b && b.metadataBadgeRenderer && b.metadataBadgeRenderer.label) {
          durationText = String(b.metadataBadgeRenderer.label);
          break;
        }
      }
    }
    var duration = parseDurationText(durationText);
    var thumbRaw = null;
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.musicThumbnailRenderer && c.thumbnail.musicThumbnailRenderer.thumbnail && c.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.video && c.video.thumbnail && c.video.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnail && c.thumbnail.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnails && c.thumbnail.thumbnails) || null);
    var thumb = makeSquareThumb(thumbRaw);
    thumb = normalizeUrl(thumb) || null;
    return {
      id: String(videoId),
      title: String(title),
      artist: String(artist || ""),
      album: "",
      duration: Number(duration || 0),
      thumbnail: thumb,
      source: "youtube"
    };
  } catch (e) {
    L("warn", "parseItemExtended error", String(e));
    return null;
  }
}

function collectItemsFromNode(node, out) {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (var i = 0; i < node.length; i++) collectItemsFromNode(node[i], out);
    return;
  }
  if (node.videoRenderer || node.musicResponsiveListItemRenderer || node.musicTwoRowItemRenderer || node.musicCardRenderer || (node.richItemRenderer && node.richItemRenderer.content) || node.playlistPanelVideoRenderer) {
    out.push(node);
  }
  for (var k in node) {
    if (!Object.prototype.hasOwnProperty.call(node, k)) continue;
    var v = node[k];
    if (!v) continue;
    if (Array.isArray(v)) {
      for (var ai = 0; ai < v.length; ai++) collectItemsFromNode(v[ai], out);
    } else if (typeof v === "object") {
      collectItemsFromNode(v, out);
    }
  }
}

function parseSearchResponseExtended(data) {
  try {
    if (!data || typeof data !== "object") return [];
    var rootCandidates = [];
    if (Array.isArray(data.onResponseReceivedCommands)) {
      data.onResponseReceivedCommands.forEach(function(cmd){
        if (cmd && typeof cmd === "object") {
          if (cmd.appendContinuationItemsAction && cmd.appendContinuationItemsAction.continuationItems) {
            collectItemsFromNode(cmd.appendContinuationItemsAction.continuationItems, rootCandidates);
          }
          collectItemsFromNode(cmd, rootCandidates);
        }
      });
    }
    if (data.onResponseReceivedActions) collectItemsFromNode(data.onResponseReceivedActions, rootCandidates);
    if (data.contents) collectItemsFromNode(data.contents, rootCandidates);
    if (data.results) collectItemsFromNode(data.results, rootCandidates);
    collectItemsFromNode(data, rootCandidates);
    var results = [];
    for (var i = 0; i < rootCandidates.length; i++) {
      var node = rootCandidates[i];
      var possible = node.musicResponsiveListItemRenderer || node.musicTwoRowItemRenderer || node.musicCardRenderer || (node.richItemRenderer && node.richItemRenderer.content) || node.videoRenderer || node;
      var parsed = parseItemExtended(possible);
      if (parsed) results.push(parsed);
    }
    var seen = {};
    var deduped = [];
    for (var r = 0; r < results.length; r++) {
      var item = results[r];
      if (!item || !item.id) continue;
      if (!seen[item.id]) {
        seen[item.id] = true;
        deduped.push(item);
        if (deduped.length >= CONFIG.maxResults) break;
      }
    }
    return deduped;
  } catch (e) {
    L("error", "parseSearchResponseExtended fatal", String(e));
    return [];
  }
}

var URL_KEY_RE = /url|uri|link|cover|download|thumbnail/i;

function stripUrlLikeFields(obj) {
  var out = {};
  for (var k in obj) {
    if (!Object.prototype.hasOwnProperty.call(obj, k)) continue;
    var v = obj[k];
    if (URL_KEY_RE.test(k)) {
      if (isString(v) && isAbsoluteHttpUrl(v)) {
        out[k] = normalizeUrl(v);
      } else {
        out[k] = null;
      }
    } else {
      out[k] = v;
    }
  }
  return out;
}

function sanitizeTrackBeforeReturn(t) {
  if (!t || typeof t !== "object") return null;
  var id = t.id ? String(t.id).trim() : "";
  if (!id) return null;
  var title = t.title ? String(t.title).trim() : "Unknown title";
  var artist = t.artist ? String(t.artist).trim() : "";
  var thumbCandidate = t.thumbnail || t.coverUrl || null;
  var thumb = normalizeUrl(thumbCandidate) || null;
  var base = {
    id: id,
    title: title,
    artist: artist,
    album: t.album ? String(t.album).trim() : "",
    duration: Number(t.duration || 0) || 0,
    thumbnail: thumb,
    source: t.source ? String(t.source) : "youtube",
    coverUrl: null,
    downloadUrl: null,
    safeDownloadUrl: null
  };
  for (var k in t) {
    if (!Object.prototype.hasOwnProperty.call(t, k)) continue;
    if (URL_KEY_RE.test(k)) {
      var v = t[k];
      if (isString(v) && isAbsoluteHttpUrl(v)) {
      } else {
        if (isString(v) && v.trim() !== "") L("warn", "sanitizeTrackBeforeReturn cleared non-absolute url field", { id: id, key: k, value: v });
      }
    }
  }
  return base;
}

async function performSearchAsync(query) {
  var url = "https://music.youtube.com/youtubei/v1/search?alt=json";
  var body = JSON.stringify({
    context: { client: { clientName: "WEB_REMIX", clientVersion: CONFIG.clientVersion } },
    query: String(query || "")
  });
  L("info", "performSearch fetch start", query);
  var res;
  try {
    res = await safeFetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible)",
        "x-youtube-client-name": "WEB_REMIX",
        "x-youtube-client-version": CONFIG.clientVersion
      },
      body: body
    });
  } catch (e) {
    L("warn", "performSearch safeFetch failed", String(e));
    return [];
  }
  L("debug", "performSearch http status", res.status);
  var rawText = "";
  try {
    rawText = await res.text();
    L("debug", "performSearch raw text head", rawText.slice(0, CONFIG.debugRawJsonHead));
  } catch (e) {
    L("warn", "performSearch read text failed", String(e));
    return [];
  }
  var data;
  try { data = JSON.parse(rawText); } catch (e) { L("error", "performSearch json parse failed", String(e)); return []; }
  var parsed = parseSearchResponseExtended(data);
  L("info", "performSearch parsed results", parsed.length);
  return parsed;
}

function customSearchSync(query) {
  var key = "yt:search:" + String(query || "");
  var cached = cacheGet(key);
  if (cached) return cached;
  dedupFetch(key, async function() {
    try {
      var results = await performSearchAsync(query);
      if (Array.isArray(results) && results.length > 0) {
        var sanitized = results.map(sanitizeTrackBeforeReturn).filter(function(x){ return !!x; }).map(stripUrlLikeFields);
        if (sanitized.length > 0) {
          cacheSet(key, sanitized);
          L("info", "customSearch cached results", query, sanitized.length);
        } else {
          L("info", "customSearch sanitized to zero items", query);
        }
      } else {
        L("info", "customSearch no results to cache", query);
      }
    } catch (e) {
      L("warn", "customSearch background fetch failed", String(e));
    }
  }).catch(function(){});
  return [{
    id: "FALLBACK-" + String(query || "q"),
    title: "Loading results for " + String(query || ""),
    artist: "",
    album: "",
    duration: 0,
    thumbnail: null,
    source: "youtube",
    coverUrl: null,
    downloadUrl: null,
    safeDownloadUrl: null
  }];
}

function validateTrackForDownload(track) {
  if (!track || typeof track !== "object") return { ok: false, reason: "invalid_track" };
  if (!track.id || !String(track.id).trim()) return { ok: false, reason: "missing_id" };
  var keys = ["downloadUrl", "coverUrl", "thumbnail", "url", "uri"];
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    if (Object.prototype.hasOwnProperty.call(track, k)) {
      var v = track[k];
      if (v === "") {
        return { ok: false, reason: k + "_empty" };
      }
      if (v === null || typeof v === "undefined") {
        continue;
      }
      if (v && !isAbsoluteHttpUrl(v)) return { ok: false, reason: k + "_invalid" };
    }
  }
  return { ok: true };
}

function finalGuardBeforeNative(track) {
  var v = validateTrackForDownload(track);
  if (!v.ok) {
    L("error", "native call blocked invalid field", v.reason, track && track.id);
    try {
      if (typeof DEBUG !== "undefined" && DEBUG) {
        var offending = {};
        var keys = ["downloadUrl", "coverUrl", "thumbnail", "url", "uri"];
        for (var i = 0; i < keys.length; i++) {
          var k = keys[i];
          if (Object.prototype.hasOwnProperty.call(track, k)) offending[k] = track[k];
        }
        L("debug", "finalGuard offending fields", offending);
      }
    } catch (e) {}
    return false;
  }
  return true;
}

registerExtension({
  initialize: function() { L("info", "YT sanitize init"); return true; },
  customSearch: function(query) {
    L("info", "YT sanitize customSearch", query);
    try { return customSearchSync(query); } catch (e) {
      L("error", "customSearch fatal", String(e));
      return [{
        id: "ERROR-" + String(query || "q"),
        title: "Error loading results",
        artist: "",
        album: "",
        duration: 0,
        thumbnail: null,
        source: "youtube",
        coverUrl: null,
        downloadUrl: null,
        safeDownloadUrl: null
      }];
    }
  },
  validateTrackForDownload: validateTrackForDownload,
  finalGuardBeforeNative: finalGuardBeforeNative,
  matchTrack: function() { return null; },
  checkAvailability: function() { return false; },
  getDownloadUrl: function() { return null; },
  download: function() { return null; },
  cleanup: function() { L("info", "YT sanitize cleanup"); return true; }
});