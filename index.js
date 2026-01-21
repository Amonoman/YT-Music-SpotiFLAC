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

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36"
];

function getRandomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

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

function extractBrowseInfoFromEndpoint(ep) {
  try {
    if (!ep) return null;
    if (ep.browseEndpoint && ep.browseEndpoint.browseId) {
      var browseId = ep.browseEndpoint.browseId;
      var type = "unknown";
      if (browseId.startsWith("MPREb_")) type = "album";
      else if (browseId.startsWith("VLPL") || browseId.startsWith("VL") || browseId.startsWith("PL")) type = "playlist";
      else if (browseId.startsWith("VLRDCLAK5uy_")) type = "playlist";
      else if (browseId.startsWith("UC")) type = "artist";
      return { browseId: browseId, type: type };
    }
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
    if (c.flexColumns && Array.isArray(c.flexColumns)) {
      for (var fi = 0; fi < c.flexColumns.length; fi++) {
        var fc = c.flexColumns[fi];
        if (fc && fc.musicResponsiveListItemFlexColumnRenderer) {
          var fcr = fc.musicResponsiveListItemFlexColumnRenderer;
          if (fcr.text && fcr.text.runs && fcr.text.runs[0]) {
            if (!title) title = fcr.text.runs[0].text;
          }
        }
      }
    }
    
    if (!title && c.title && c.title.runs && c.title.runs[0] && c.title.runs[0].text) title = c.title.runs[0].text;
    if (!title && c.title && c.title.simpleText) title = c.title.simpleText;
    if (!title && c.titleText && c.titleText.runs && c.titleText.runs[0] && c.titleText.runs[0].text) title = c.titleText.runs[0].text;
    if (!title && c.name && c.name.simpleText) title = c.name.simpleText;
    if (!title && c.video && c.video.title) title = c.video.title;
    if (!title && c.header && c.header.title && c.header.title.runs) title = c.header.title.runs.map(function(r){return r.text;}).join(" ");
    
    var artist = "";
    if (c.flexColumns && Array.isArray(c.flexColumns) && c.flexColumns.length > 1) {
      var fc2 = c.flexColumns[1];
      if (fc2 && fc2.musicResponsiveListItemFlexColumnRenderer) {
        var fcr2 = fc2.musicResponsiveListItemFlexColumnRenderer;
        if (fcr2.text && fcr2.text.runs) {
          var artistParts = [];
          for (var ri = 0; ri < fcr2.text.runs.length; ri++) {
            var run = fcr2.text.runs[ri];
            if (run && run.text) {
              var txt = run.text.trim();
              if (txt === "•" || txt === " • " || txt === "," || txt === " & ") continue;
              var lowerTxt = txt.toLowerCase();
              if (lowerTxt === "single" || lowerTxt === "album" || lowerTxt === "ep" || 
                  lowerTxt === "playlist" || lowerTxt === "video" || lowerTxt === "song") continue;
              if (/^\d{4}$/.test(txt)) continue;
              if (/^\d+(\.\d+)?[KMB]?\s*(views|plays|listeners|subscribers|monthly audience)/i.test(txt)) continue;
              if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(txt)) continue;
              if (run.navigationEndpoint && run.navigationEndpoint.browseEndpoint) {
                artistParts.push(txt);
              } else if (!run.navigationEndpoint) {
                if (txt.length > 1) artistParts.push(txt);
              }
            }
          }
          artist = artistParts.join(", ");
        }
      }
    }
    
    if (!artist && c.subtitle && c.subtitle.runs) {
      var subtitleParts = [];
      for (var si = 0; si < c.subtitle.runs.length; si++) {
        var srun = c.subtitle.runs[si];
        if (srun && srun.text) {
          var stxt = srun.text.trim();
          if (stxt === "•" || stxt === " • " || stxt === "," || stxt === " & ") continue;
          var lowerStxt = stxt.toLowerCase();
          if (lowerStxt === "single" || lowerStxt === "album" || lowerStxt === "ep" || 
              lowerStxt === "playlist" || lowerStxt === "video" || lowerStxt === "song") continue;
          if (/^\d{4}$/.test(stxt)) continue;
          // Skip view/play counts (e.g., "159M views", "1.1B plays", "241M plays")
          if (/^\d+(\.\d+)?[KMB]?\s*(views|plays|listeners|subscribers|monthly audience)/i.test(stxt)) continue;
          // Skip duration format (e.g., "3:06", "10:45", "1:23:45")
          if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(stxt)) continue;
          if (stxt.length > 1) subtitleParts.push(stxt);
        }
      }
      artist = subtitleParts.join(", ");
    }
    if (!artist && c.longBylineText && c.longBylineText.runs) artist = c.longBylineText.runs.map(function(r){ return r.text; }).join(" ");
    if (!artist && c.ownerText && c.ownerText.runs) artist = c.ownerText.runs.map(function(r){ return r.text; }).join(" ");
    
    if (!artist) {
      L("debug", "parseItemExtended: no artist found for", title);
    }
    
    var album = "";
    if (c.flexColumns && Array.isArray(c.flexColumns) && c.flexColumns.length > 1) {
      var fc2 = c.flexColumns[1];
      if (fc2 && fc2.musicResponsiveListItemFlexColumnRenderer) {
        var fcr2 = fc2.musicResponsiveListItemFlexColumnRenderer;
        if (fcr2.text && fcr2.text.runs) {
          for (var ri = 0; ri < fcr2.text.runs.length; ri++) {
            var run = fcr2.text.runs[ri];
            if (run && run.text && run.navigationEndpoint && run.navigationEndpoint.browseEndpoint) {
              var browseId = run.navigationEndpoint.browseEndpoint.browseId || "";
              if (browseId.startsWith("MPREb_")) {
                album = run.text.trim();
                L("debug", "parseItemExtended: found album from flexColumns", album);
                break;
              }
            }
          }
        }
      }
    }
    
    if (!album && c.subtitle && c.subtitle.runs) {
      for (var si = 0; si < c.subtitle.runs.length; si++) {
        var srun = c.subtitle.runs[si];
        if (srun && srun.text && srun.navigationEndpoint && srun.navigationEndpoint.browseEndpoint) {
          var sBrowseId = srun.navigationEndpoint.browseEndpoint.browseId || "";
          if (sBrowseId.startsWith("MPREb_")) {
            album = srun.text.trim();
            L("debug", "parseItemExtended: found album from subtitle", album);
            break;
          }
        }
      }
    }
    
    var videoId = null;
    if (c.playlistItemData && c.playlistItemData.videoId) videoId = c.playlistItemData.videoId;
    if (!videoId && c.videoId) videoId = c.videoId;
    if (!videoId && c.overlay && c.overlay.musicItemThumbnailOverlayRenderer && c.overlay.musicItemThumbnailOverlayRenderer.content && c.overlay.musicItemThumbnailOverlayRenderer.content.musicPlayButtonRenderer) {
      var mpbr = c.overlay.musicItemThumbnailOverlayRenderer.content.musicPlayButtonRenderer;
      if (mpbr.playNavigationEndpoint) videoId = extractVideoIdFromEndpoint(mpbr.playNavigationEndpoint);
    }
    if (!videoId && c.navigationEndpoint) videoId = extractVideoIdFromEndpoint(c.navigationEndpoint);
    if (!videoId && c.thumbnail && c.thumbnail.musicThumbnailRenderer && c.thumbnail.musicThumbnailRenderer.navigationEndpoint) videoId = extractVideoIdFromEndpoint(c.thumbnail.musicThumbnailRenderer.navigationEndpoint);
    if (!videoId && c.video && c.video.videoId) videoId = c.video.videoId;
    
    if (!title || !videoId) {
      if (Math.random() < 0.1) L("debug", "parseItemExtended failed", { hasTitle: !!title, hasVideoId: !!videoId, keys: Object.keys(c).slice(0, 5) });
      return null;
    }
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
    if (!durationText && c.fixedColumns && Array.isArray(c.fixedColumns)) {
      for (var dfi = 0; dfi < c.fixedColumns.length; dfi++) {
        var dfc = c.fixedColumns[dfi];
        if (dfc && dfc.musicResponsiveListItemFixedColumnRenderer) {
          var dfcr = dfc.musicResponsiveListItemFixedColumnRenderer;
          if (dfcr.text && dfcr.text.runs && dfcr.text.runs[0] && dfcr.text.runs[0].text) {
            var possibleDuration = dfcr.text.runs[0].text.trim();
            if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(possibleDuration)) {
              durationText = possibleDuration;
              L("debug", "parseItemExtended: found duration from fixedColumns", durationText);
              break;
            }
          }
          if (!durationText && dfcr.text && dfcr.text.simpleText) {
            var possibleDuration2 = dfcr.text.simpleText.trim();
            if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(possibleDuration2)) {
              durationText = possibleDuration2;
              L("debug", "parseItemExtended: found duration from fixedColumns simpleText", durationText);
              break;
            }
          }
        }
      }
    }
    if (!durationText && c.flexColumns && Array.isArray(c.flexColumns) && c.flexColumns.length > 1) {
      var fc3 = c.flexColumns[1];
      if (fc3 && fc3.musicResponsiveListItemFlexColumnRenderer) {
        var fcr3 = fc3.musicResponsiveListItemFlexColumnRenderer;
        if (fcr3.text && fcr3.text.runs) {
          for (var dri = 0; dri < fcr3.text.runs.length; dri++) {
            var drun = fcr3.text.runs[dri];
            if (drun && drun.text) {
              var dtxt = drun.text.trim();
              if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(dtxt)) {
                durationText = dtxt;
                L("debug", "parseItemExtended: found duration from flexColumns", durationText);
                break;
              }
            }
          }
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
    if (!thumbRaw && c.fixedColumns) {
      for (var fi = 0; fi < c.fixedColumns.length; fi++) {
        var fc = c.fixedColumns[fi];
        if (fc && fc.musicResponsiveListItemFixedColumnRenderer && fc.musicResponsiveListItemFixedColumnRenderer.thumbnail) {
          thumbRaw = pickLastThumbnailUrl(fc.musicResponsiveListItemFixedColumnRenderer.thumbnail.thumbnails);
          if (thumbRaw) break;
        }
      }
    }
    var thumb = makeSquareThumb(thumbRaw);
    thumb = normalizeUrl(thumb) || null;
    return {
      id: String(videoId),
      title: String(title),
      artist: String(artist || ""),
      album: String(album || ""),
      duration: Number(duration || 0),
      thumbnail: thumb,
      source: "youtube",
      item_type: "track"
    };
  } catch (e) {
    L("warn", "parseItemExtended error", String(e));
    return null;
  }
}

function parseCollectionItem(info) {
  try {
    if (!info) return null;
    var c = normalizeCandidate(info);
    if (!c) return null;
    
    var browseInfo = null;
    if (c.navigationEndpoint) {
      browseInfo = extractBrowseInfoFromEndpoint(c.navigationEndpoint);
    }
    if (!browseInfo && c.overlay && c.overlay.musicItemThumbnailOverlayRenderer) {
      var overlay = c.overlay.musicItemThumbnailOverlayRenderer;
      if (overlay.content && overlay.content.musicPlayButtonRenderer && overlay.content.musicPlayButtonRenderer.playNavigationEndpoint) {
        browseInfo = extractBrowseInfoFromEndpoint(overlay.content.musicPlayButtonRenderer.playNavigationEndpoint);
      }
    }
    
    // Only process albums, playlists, and artists
    if (!browseInfo || (browseInfo.type !== "album" && browseInfo.type !== "playlist" && browseInfo.type !== "artist")) {
      return null;
    }
    
    // Extract title
    var title = null;
    if (c.flexColumns && Array.isArray(c.flexColumns)) {
      for (var fi = 0; fi < c.flexColumns.length; fi++) {
        var fc = c.flexColumns[fi];
        if (fc && fc.musicResponsiveListItemFlexColumnRenderer) {
          var fcr = fc.musicResponsiveListItemFlexColumnRenderer;
          if (fcr.text && fcr.text.runs && fcr.text.runs[0]) {
            if (!title) title = fcr.text.runs[0].text;
          }
        }
      }
    }
    if (!title && c.title && c.title.runs && c.title.runs[0]) title = c.title.runs[0].text;
    if (!title && c.title && c.title.simpleText) title = c.title.simpleText;
    
    var subtitle = "";
    var albumType = browseInfo.type === "album" ? "album" : "playlist";
    var year = "";
    var artist = "";
    
    if (c.flexColumns && Array.isArray(c.flexColum