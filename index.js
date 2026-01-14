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

// Random user agents to avoid detection/banning
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

// Extract browseId and determine type from endpoint
function extractBrowseInfoFromEndpoint(ep) {
  try {
    if (!ep) return null;
    if (ep.browseEndpoint && ep.browseEndpoint.browseId) {
      var browseId = ep.browseEndpoint.browseId;
      var type = "unknown";
      // YouTube Music browse IDs have prefixes:
      // MPREb_ = Album/EP/Single
      // VLPL or VL = Playlist
      // VLRDCLAK5uy_ = Radio/Auto playlist
      // UC = Channel/Artist
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
    
    // Try to extract title from flexColumns (YouTube Music specific)
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
    
    // Try to extract artist from flexColumns (usually second column)
    var artist = "";
    if (c.flexColumns && Array.isArray(c.flexColumns) && c.flexColumns.length > 1) {
      var fc2 = c.flexColumns[1];
      if (fc2 && fc2.musicResponsiveListItemFlexColumnRenderer) {
        var fcr2 = fc2.musicResponsiveListItemFlexColumnRenderer;
        if (fcr2.text && fcr2.text.runs) {
          // Extract artist names, filtering out type indicators and separators
          var artistParts = [];
          for (var ri = 0; ri < fcr2.text.runs.length; ri++) {
            var run = fcr2.text.runs[ri];
            if (run && run.text) {
              var txt = run.text.trim();
              // Skip type indicators (Single, Album, EP, etc.) and separators
              if (txt === "•" || txt === " • " || txt === "," || txt === " & ") continue;
              var lowerTxt = txt.toLowerCase();
              if (lowerTxt === "single" || lowerTxt === "album" || lowerTxt === "ep" || 
                  lowerTxt === "playlist" || lowerTxt === "video" || lowerTxt === "song") continue;
              // Skip if it's a year (4 digits)
              if (/^\d{4}$/.test(txt)) continue;
              // Skip view/play counts (e.g., "159M views", "1.1B plays", "241M plays")
              if (/^\d+(\.\d+)?[KMB]?\s*(views|plays|listeners|subscribers|monthly audience)/i.test(txt)) continue;
              // Skip duration format (e.g., "3:06", "10:45", "1:23:45")
              if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(txt)) continue;
              // Check if run has navigationEndpoint with browseId (indicates it's an artist link)
              if (run.navigationEndpoint && run.navigationEndpoint.browseEndpoint) {
                artistParts.push(txt);
              } else if (!run.navigationEndpoint) {
                // Plain text that's not a link - could be artist or separator
                if (txt.length > 1) artistParts.push(txt);
              }
            }
          }
          artist = artistParts.join(", ");
        }
      }
    }
    
    if (!artist && c.subtitle && c.subtitle.runs) {
      // Same filtering for subtitle
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
    
    // Debug: log parsed artist
    if (!artist) {
      L("debug", "parseItemExtended: no artist found for", title);
    }
    
    // Extract videoId from various locations
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
      // Debug: log first few failed candidates
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
    var duration = parseDurationText(durationText);
    var thumbRaw = null;
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.musicThumbnailRenderer && c.thumbnail.musicThumbnailRenderer.thumbnail && c.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.video && c.video.thumbnail && c.video.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnail && c.thumbnail.thumbnail.thumbnails) || null);
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnails && c.thumbnail.thumbnails) || null);
    // Try to get thumbnail from fixedColumns (album/playlist tracks)
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
      album: "",
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

// Parse album/playlist/EP items from search results
function parseCollectionItem(info) {
  try {
    if (!info) return null;
    var c = normalizeCandidate(info);
    if (!c) return null;
    
    // Check if this is an album/playlist by looking for browseEndpoint
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
    
    // Extract subtitle (artist • type • year)
    var subtitle = "";
    var albumType = browseInfo.type === "album" ? "album" : "playlist";
    var year = "";
    var artist = "";
    
    if (c.flexColumns && Array.isArray(c.flexColumns) && c.flexColumns.length > 1) {
      var fc2 = c.flexColumns[1];
      if (fc2 && fc2.musicResponsiveListItemFlexColumnRenderer) {
        var fcr2 = fc2.musicResponsiveListItemFlexColumnRenderer;
        if (fcr2.text && fcr2.text.runs) {
          subtitle = fcr2.text.runs.map(function(r){ return r.text; }).join("");
        }
      }
    }
    if (!subtitle && c.subtitle && c.subtitle.runs) {
      subtitle = c.subtitle.runs.map(function(r){ return r.text; }).join("");
    }
    
    // Parse subtitle to extract type and year
    // Format: "Album • Artist • 2024" or "Playlist • Owner • 123K views"
    if (subtitle) {
      var parts = subtitle.split(" • ");
      if (parts.length >= 1) {
        var typeStr = parts[0].toLowerCase().trim();
        if (typeStr === "album" || typeStr === "single" || typeStr === "ep") {
          albumType = typeStr === "ep" ? "ep" : typeStr;
        } else if (typeStr === "playlist") {
          albumType = "playlist";
        }
      }
      if (parts.length >= 2) {
        artist = parts[1].trim();
      }
      if (parts.length >= 3) {
        var maybeYear = parts[2].trim();
        if (/^\d{4}$/.test(maybeYear)) {
          year = maybeYear;
        }
      }
    }
    
    // Extract thumbnail - check multiple structures
    var thumbRaw = null;
    // musicResponsiveListItemRenderer structure
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.musicThumbnailRenderer && c.thumbnail.musicThumbnailRenderer.thumbnail && c.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails) || null);
    // Direct thumbnails array
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnail && c.thumbnail.thumbnails) || null);
    // musicTwoRowItemRenderer structure (used on artist pages)
    thumbRaw = thumbRaw || pickLastThumbnailUrl((c.thumbnailRenderer && c.thumbnailRenderer.musicThumbnailRenderer && c.thumbnailRenderer.musicThumbnailRenderer.thumbnail && c.thumbnailRenderer.musicThumbnailRenderer.thumbnail.thumbnails) || null);
    // Alternative musicThumbnailRenderer location
    if (!thumbRaw && c.thumbnail && c.thumbnail.musicThumbnailRenderer) {
      var mtr = c.thumbnail.musicThumbnailRenderer;
      if (mtr.thumbnail && mtr.thumbnail.thumbnails) {
        thumbRaw = pickLastThumbnailUrl(mtr.thumbnail.thumbnails);
      }
    }
    var thumb = makeSquareThumb(thumbRaw);
    thumb = normalizeUrl(thumb) || null;
    L("debug", "parseCollectionItem thumbnail", { id: browseInfo ? browseInfo.browseId : "null", thumbRaw: thumbRaw, thumb: thumb });
    
    if (!title || !browseInfo.browseId) {
      return null;
    }
    
    // Determine item_type
    var itemType = "album";
    if (browseInfo.type === "playlist") itemType = "playlist";
    else if (browseInfo.type === "artist") itemType = "artist";
    
    L("info", "parseCollectionItem returning", { id: browseInfo.browseId, title: title, item_type: itemType, browseType: browseInfo.type });
    
    return {
      id: browseInfo.browseId,
      title: String(title),
      artist: String(artist || ""),
      album_type: browseInfo.type === "artist" ? "artist" : albumType,
      year: year,
      thumbnail: thumb,
      item_type: itemType,
      source: "youtube"
    };
  } catch (e) {
    L("warn", "parseCollectionItem error", String(e));
    return null;
  }
}

// Parse both tracks and collections from search results
// Parse artist card from musicCardShelfRenderer (the featured artist at top of search)
function parseArtistCardShelf(info) {
  try {
    if (!info || !info.musicCardShelfRenderer) return null;
    var c = info.musicCardShelfRenderer;
    
    // Extract artist name from title
    var title = null;
    if (c.title && c.title.runs && c.title.runs[0]) {
      title = c.title.runs[0].text;
    }
    
    // Extract browseId from various navigation endpoints
    var browseId = null;
    if (c.title && c.title.runs && c.title.runs[0] && c.title.runs[0].navigationEndpoint) {
      var navEp = c.title.runs[0].navigationEndpoint;
      if (navEp.browseEndpoint && navEp.browseEndpoint.browseId) {
        browseId = navEp.browseEndpoint.browseId;
      }
    }
    // Alternative: check onTap or buttons
    if (!browseId && c.onTap && c.onTap.browseEndpoint) {
      browseId = c.onTap.browseEndpoint.browseId;
    }
    if (!browseId && c.buttons && c.buttons.length > 0) {
      for (var bi = 0; bi < c.buttons.length; bi++) {
        var btn = c.buttons[bi];
        if (btn && btn.buttonRenderer && btn.buttonRenderer.navigationEndpoint && btn.buttonRenderer.navigationEndpoint.browseEndpoint) {
          browseId = btn.buttonRenderer.navigationEndpoint.browseEndpoint.browseId;
          break;
        }
      }
    }
    
    // Check if this is an artist (browseId starts with UC)
    if (!browseId || !browseId.startsWith("UC")) {
      return null;
    }
    
    if (!title) return null;
    
    // Extract subtitle (e.g., "45M monthly audience")
    var subtitle = "";
    if (c.subtitle && c.subtitle.runs) {
      subtitle = c.subtitle.runs.map(function(r){ return r.text; }).join("");
    }
    
    // Extract thumbnail
    var thumbRaw = null;
    if (c.thumbnail && c.thumbnail.musicThumbnailRenderer && c.thumbnail.musicThumbnailRenderer.thumbnail) {
      thumbRaw = pickLastThumbnailUrl(c.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails);
    }
    var thumb = makeSquareThumb(thumbRaw);
    thumb = normalizeUrl(thumb) || null;
    
    L("info", "parseArtistCardShelf found artist", { id: browseId, name: title, subtitle: subtitle });
    
    return {
      id: browseId,
      title: String(title),
      artist: String(title), // For artists, artist name is the title
      thumbnail: thumb,
      item_type: "artist",
      album_type: "artist",
      source: "youtube"
    };
  } catch (e) {
    L("warn", "parseArtistCardShelf error", String(e));
    return null;
  }
}

function parseSearchItem(info) {
  // First check for musicCardShelfRenderer (featured artist card at top of search)
  if (info && info.musicCardShelfRenderer) {
    var artistCard = parseArtistCardShelf(info);
    if (artistCard) return artistCard;
  }
  
  // Then try to parse as collection (album/playlist/artist)
  var collection = parseCollectionItem(info);
  if (collection) return collection;
  
  // Otherwise parse as track
  return parseItemExtended(info);
}

function collectItemsFromNode(node, out, depth) {
  depth = depth || 0;
  // Prevent infinite recursion - max depth 20
  if (depth > 20) return;
  // Early exit if we have enough items
  if (out.length >= 100) return;
  
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (var i = 0; i < node.length && out.length < 100; i++) collectItemsFromNode(node[i], out, depth + 1);
    return;
  }
  if (node.videoRenderer || node.musicResponsiveListItemRenderer || node.musicTwoRowItemRenderer || node.musicCardRenderer || (node.richItemRenderer && node.richItemRenderer.content) || node.playlistPanelVideoRenderer || node.musicCardShelfRenderer) {
    out.push(node);
  }
  for (var k in node) {
    if (!Object.prototype.hasOwnProperty.call(node, k)) continue;
    if (out.length >= 100) break;
    var v = node[k];
    if (!v) continue;
    if (Array.isArray(v)) {
      for (var ai = 0; ai < v.length && out.length < 100; ai++) collectItemsFromNode(v[ai], out, depth + 1);
    } else if (typeof v === "object") {
      collectItemsFromNode(v, out, depth + 1);
    }
  }
}

// Collect ONLY album/playlist tracks from the first musicShelfRenderer (ignore "Related", "You might also like" sections)
function collectAlbumTracksOnly(data, out) {
  if (!data || typeof data !== "object") return;
  
  // Find the first musicShelfRenderer which contains album tracks
  var shelfFound = false;
  
  function findFirstShelf(node, depth) {
    if (shelfFound || depth > 15) return;
    if (!node || typeof node !== "object") return;
    
    if (Array.isArray(node)) {
      for (var i = 0; i < node.length; i++) {
        findFirstShelf(node[i], depth + 1);
        if (shelfFound) return;
      }
      return;
    }
    
    // Found a musicShelfRenderer - this should be the track list
    if (node.musicShelfRenderer && node.musicShelfRenderer.contents) {
      shelfFound = true;
      var contents = node.musicShelfRenderer.contents;
      for (var i = 0; i < contents.length; i++) {
        var item = contents[i];
        if (item.musicResponsiveListItemRenderer) {
          out.push(item);
        }
      }
      return;
    }
    
    // Also check for playlistPanelRenderer (for some playlists)
    if (node.playlistPanelRenderer && node.playlistPanelRenderer.contents) {
      shelfFound = true;
      var contents = node.playlistPanelRenderer.contents;
      for (var i = 0; i < contents.length; i++) {
        var item = contents[i];
        if (item.playlistPanelVideoRenderer) {
          out.push(item);
        }
      }
      return;
    }
    
    // Recurse into object properties
    for (var k in node) {
      if (!Object.prototype.hasOwnProperty.call(node, k)) continue;
      if (shelfFound) return;
      var v = node[k];
      if (v && typeof v === "object") {
        findFirstShelf(v, depth + 1);
      }
    }
  }
  
  findFirstShelf(data, 0);
  L("debug", "collectAlbumTracksOnly found", out.length, "tracks, shelfFound:", shelfFound);
}

function parseSearchResponseExtended(data) {
  try {
    if (!data || typeof data !== "object") return [];
    var rootCandidates = [];
    
    // Handle tabbedSearchResultsRenderer (new YouTube Music structure)
    if (data.contents && data.contents.tabbedSearchResultsRenderer) {
      var tabs = data.contents.tabbedSearchResultsRenderer.tabs;
      if (Array.isArray(tabs)) {
        for (var ti = 0; ti < tabs.length; ti++) {
          var tab = tabs[ti];
          if (tab && tab.tabRenderer && tab.tabRenderer.content) {
            collectItemsFromNode(tab.tabRenderer.content, rootCandidates);
          }
        }
      }
    }
    
    // Handle sectionListRenderer directly
    if (data.contents && data.contents.sectionListRenderer) {
      collectItemsFromNode(data.contents.sectionListRenderer, rootCandidates);
    }
    
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
    
    // Only do full tree scan if we found nothing
    if (rootCandidates.length === 0) {
      collectItemsFromNode(data, rootCandidates);
    }
    
    L("debug", "parseSearchResponseExtended candidates", rootCandidates.length);
    
    var results = [];
    for (var i = 0; i < rootCandidates.length; i++) {
      var node = rootCandidates[i];
      var possible = node.musicResponsiveListItemRenderer || node.musicTwoRowItemRenderer || node.musicCardRenderer || (node.richItemRenderer && node.richItemRenderer.content) || node.videoRenderer || node;
      // Parse both tracks and collections (albums/playlists)
      var parsed = parseSearchItem(possible);
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
  
  // If this is a collection (album/playlist/artist), use different sanitization
  if (t.item_type === "album" || t.item_type === "playlist" || t.item_type === "artist") {
    return sanitizeCollectionBeforeReturn(t);
  }
  
  var title = t.title ? String(t.title).trim() : "Unknown title";
  var artist = t.artist ? String(t.artist).trim() : "";
  var thumbCandidate = t.thumbnail || t.coverUrl || null;
  var thumb = normalizeUrl(thumbCandidate) || null;
  
  // Map to SpotiFLAC ExtTrackMetadata format
  return {
    id: id,
    name: title,                              // SpotiFLAC expects 'name' not 'title'
    artists: artist,                          // SpotiFLAC expects 'artists' not 'artist'
    album_name: t.album ? String(t.album).trim() : "",
    duration_ms: (Number(t.duration || 0) || 0) * 1000, // Convert seconds to ms
    cover_url: thumb,                         // SpotiFLAC expects 'cover_url' not 'thumbnail'
    provider_id: "ytmusic-spotiflac",
    item_type: "track"
  };
}

// Sanitize album/playlist collection for return
function sanitizeCollectionBeforeReturn(t) {
  if (!t || typeof t !== "object") return null;
  var id = t.id ? String(t.id).trim() : "";
  if (!id) return null;
  
  var title = t.title ? String(t.title).trim() : "Unknown";
  var artist = t.artist ? String(t.artist).trim() : "";
  var thumbCandidate = t.thumbnail || t.coverUrl || null;
  var thumb = normalizeUrl(thumbCandidate) || null;
  
  var finalItemType = t.item_type || "album";
  L("info", "sanitizeCollectionBeforeReturn", { id: id, name: title, input_item_type: t.item_type, final_item_type: finalItemType });
  
  return {
    id: id,
    name: title,
    artists: artist,
    album_name: title, // For albums, album_name is the title
    album_type: t.album_type || (t.item_type === "playlist" ? "playlist" : "album"),
    release_date: t.year || "",
    cover_url: thumb,
    provider_id: "ytmusic-spotiflac",
    item_type: finalItemType
  };
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

function performSearchSync(query) {
  var url = "https://music.youtube.com/youtubei/v1/search?alt=json";
  var body = JSON.stringify({
    context: { client: { clientName: "WEB_REMIX", clientVersion: CONFIG.clientVersion } },
    query: String(query || "")
  });
  
  L("info", "performSearchSync fetch start", query);
  
  var res;
  try {
    res = fetch(url, {
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
    L("error", "performSearchSync fetch failed", String(e));
    return [];
  }
  
  if (!res || !res.ok) {
    L("error", "performSearchSync bad response", res ? res.status : "no response");
    return [];
  }
  
  L("debug", "performSearchSync http status", res.status);
  
  var data;
  try {
    data = res.json();
  } catch (e) {
    L("error", "performSearchSync json parse failed", String(e));
    return [];
  }
  
  var parsed = parseSearchResponseExtended(data);
  L("info", "performSearchSync parsed results", parsed.length);
  return parsed;
}

function customSearchSync(query) {
  var key = "yt:search:" + String(query || "");
  var cached = cacheGet(key);
  if (cached) {
    L("info", "customSearch returning cached", query, cached.length);
    return cached;
  }
  
  try {
    var results = performSearchSync(query);
    if (Array.isArray(results) && results.length > 0) {
      var sanitized = results.map(sanitizeTrackBeforeReturn).filter(function(x){ return !!x; }).map(stripUrlLikeFields);
      if (sanitized.length > 0) {
        cacheSet(key, sanitized);
        L("info", "customSearch returning results", query, sanitized.length);
        return sanitized;
      }
    }
    L("info", "customSearch no results", query);
    return [];
  } catch (e) {
    L("error", "customSearch failed", String(e));
    return [];
  }
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

// Extract video ID from YouTube URL
function extractVideoId(url) {
  if (!url) return null;
  try {
    var u = new URL(url);
    // music.youtube.com/watch?v=xxx or youtube.com/watch?v=xxx
    if (u.searchParams.has("v")) return u.searchParams.get("v");
    // youtu.be/xxx
    if (u.hostname === "youtu.be") return u.pathname.slice(1).split("/")[0];
    return null;
  } catch (e) {
    return null;
  }
}

// Extract playlist ID from YouTube URL
function extractPlaylistId(url) {
  if (!url) return null;
  try {
    var u = new URL(url);
    // music.youtube.com/playlist?list=xxx
    if (u.searchParams.has("list")) return u.searchParams.get("list");
    // music.youtube.com/browse/VLxxx (playlist browse page)
    if (u.pathname.startsWith("/browse/")) {
      var browseId = u.pathname.replace("/browse/", "");
      if (browseId.startsWith("VL") || browseId.startsWith("PL")) return browseId;
    }
    return null;
  } catch (e) {
    return null;
  }
}

// Extract album/browse ID from YouTube Music URL
function extractBrowseId(url) {
  if (!url) return null;
  try {
    var u = new URL(url);
    // music.youtube.com/browse/MPREb_xxx (album browse page)
    if (u.pathname.startsWith("/browse/")) {
      return u.pathname.replace("/browse/", "");
    }
    // music.youtube.com/playlist?list=OLAK5uy_xxx (album as playlist)
    if (u.searchParams.has("list")) {
      var list = u.searchParams.get("list");
      if (list.startsWith("OLAK5uy_")) return list;
    }
    return null;
  } catch (e) {
    return null;
  }
}

// Fetch album/playlist tracks using browse API (sync version for goja)
function fetchBrowseTracksSync(browseId) {
  L("info", "fetchBrowseTracksSync", browseId);
  
  var url = "https://music.youtube.com/youtubei/v1/browse?alt=json";
  var body = JSON.stringify({
    context: { client: { clientName: "WEB_REMIX", clientVersion: CONFIG.clientVersion } },
    browseId: browseId
  });
  
  var res;
  try {
    res = fetch(url, {
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
    L("error", "fetchBrowseTracksSync fetch failed", String(e));
    return null;
  }
  
  if (!res || !res.ok) {
    L("error", "fetchBrowseTracksSync bad response", res ? res.status : "no response");
    return null;
  }
  
  var data;
  try {
    data = res.json();
  } catch (e) {
    L("error", "fetchBrowseTracksSync json parse failed", String(e));
    return null;
  }
  
  return parseBrowseResponse(data, browseId);
}

// Parse browse response for album/playlist
function parseBrowseResponse(data, browseId) {
  try {
    if (!data) return null;
    
    // Extract header info (title, artist, thumbnail)
    var headerInfo = {
      id: browseId,
      name: "",
      artists: "",
      cover_url: null,
      album_type: "album",
      release_date: "",
      total_tracks: 0
    };
    
    // Try to find header - YouTube Music may put it in different locations
    var header = data.header;
    
    // Fallback: look for header inside contents
    if (!header && data.contents) {
      // Check singleColumnBrowseResultsRenderer
      var scbr = data.contents.singleColumnBrowseResultsRenderer;
      if (scbr && scbr.tabs && scbr.tabs[0] && scbr.tabs[0].tabRenderer && scbr.tabs[0].tabRenderer.content) {
        var tabContent = scbr.tabs[0].tabRenderer.content;
        if (tabContent.sectionListRenderer && tabContent.sectionListRenderer.header) {
          header = tabContent.sectionListRenderer.header;
        }
      }
      // Check twoColumnBrowseResultsRenderer
      var tcbr = data.contents.twoColumnBrowseResultsRenderer;
      if (!header && tcbr && tcbr.tabs && tcbr.tabs[0] && tcbr.tabs[0].tabRenderer && tcbr.tabs[0].tabRenderer.content) {
        var tabContent2 = tcbr.tabs[0].tabRenderer.content;
        if (tabContent2.sectionListRenderer && tabContent2.sectionListRenderer.header) {
          header = tabContent2.sectionListRenderer.header;
        }
      }
    }
    
    // Also check for background/foreground directly in data
    if (!header && data.background) {
      // Sometimes the thumbnail is directly in background
      var bgThumb = pickLastThumbnailUrl(data.background.musicThumbnailRenderer && data.background.musicThumbnailRenderer.thumbnail && data.background.musicThumbnailRenderer.thumbnail.thumbnails);
      if (bgThumb) {
        headerInfo.cover_url = makeSquareThumb(bgThumb);
      }
    }
    
    // Try to extract artist/title from microformat (fallback for albums without standard header)
    if (data.microformat && data.microformat.microformatDataRenderer) {
      var mf = data.microformat.microformatDataRenderer;
      if (mf.title) {
        var titleStr = mf.title;
        // Title format: "Album Name - Album by Artist" or "Album Name - Single by Artist"
        var albumByMatch = titleStr.match(/^(.+?)\s*-\s*(Album|Single|EP|Playlist)\s+by\s+(.+)$/i);
        if (albumByMatch) {
          if (!headerInfo.name) headerInfo.name = albumByMatch[1].trim();
          if (!headerInfo.artists) headerInfo.artists = albumByMatch[3].trim();
          // Detect type
          var typeStr = albumByMatch[2].toLowerCase();
          if (typeStr === "single") headerInfo.album_type = "single";
          else if (typeStr === "ep") headerInfo.album_type = "ep";
          else if (typeStr === "playlist") headerInfo.album_type = "playlist";
        } else if (!headerInfo.name) {
          headerInfo.name = titleStr;
        }
      }
      // Try to extract from description which often contains artist info
      if (!headerInfo.artists && mf.description) {
        // Description often starts with "Album • Artist • Year" or similar
        var descParts = mf.description.split(" • ");
        if (descParts.length >= 2) {
          // Skip first part if it's "Album", "Single", etc.
          var lowerFirst = descParts[0].toLowerCase().trim();
          if (lowerFirst === "album" || lowerFirst === "single" || lowerFirst === "ep" || lowerFirst === "playlist") {
            if (descParts.length >= 2) headerInfo.artists = descParts[1].trim();
          } else {
            headerInfo.artists = descParts[0].trim();
          }
        }
      }
    }
    
    // Also try frameworkUpdates for artist info
    if (!headerInfo.artists && data.frameworkUpdates && data.frameworkUpdates.entityBatchUpdate) {
      var mutations = data.frameworkUpdates.entityBatchUpdate.mutations;
      if (Array.isArray(mutations)) {
        for (var mi = 0; mi < mutations.length; mi++) {
          var mut = mutations[mi];
          if (mut && mut.payload && mut.payload.musicAlbumRelease) {
            var release = mut.payload.musicAlbumRelease;
            if (release.artistDisplayName) {
              headerInfo.artists = release.artistDisplayName;
              break;
            }
          }
        }
      }
    }
    
    L("debug", "parseBrowseResponse header keys", header ? Object.keys(header) : "no header");
    L("debug", "parseBrowseResponse data keys", Object.keys(data));
    L("debug", "parseBrowseResponse headerInfo after fallbacks", { name: headerInfo.name, artists: headerInfo.artists });
    if (header) {
      // musicDetailHeaderRenderer for albums
      if (header.musicDetailHeaderRenderer) {
        var h = header.musicDetailHeaderRenderer;
        if (h.title && h.title.runs) {
          headerInfo.name = h.title.runs.map(function(r){ return r.text; }).join("");
        }
        if (h.subtitle && h.subtitle.runs) {
          var subtitleParts = h.subtitle.runs.map(function(r){ return r.text; }).join("");
          headerInfo.artists = subtitleParts;
          // Try to extract year from subtitle
          var yearMatch = subtitleParts.match(/\d{4}/);
          if (yearMatch) headerInfo.release_date = yearMatch[0];
          // Try to detect type
          var lowerSubtitle = subtitleParts.toLowerCase();
          if (lowerSubtitle.indexOf("ep") !== -1) headerInfo.album_type = "ep";
          else if (lowerSubtitle.indexOf("single") !== -1) headerInfo.album_type = "single";
          else if (lowerSubtitle.indexOf("playlist") !== -1) headerInfo.album_type = "playlist";
        }
        // Thumbnail - try multiple paths
        if (h.thumbnail && h.thumbnail.croppedSquareThumbnailRenderer && h.thumbnail.croppedSquareThumbnailRenderer.thumbnail) {
          var thumbUrl = pickLastThumbnailUrl(h.thumbnail.croppedSquareThumbnailRenderer.thumbnail.thumbnails);
          headerInfo.cover_url = makeSquareThumb(thumbUrl);
        } else if (h.thumbnail && h.thumbnail.musicThumbnailRenderer && h.thumbnail.musicThumbnailRenderer.thumbnail) {
          var thumbUrl = pickLastThumbnailUrl(h.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails);
          headerInfo.cover_url = makeSquareThumb(thumbUrl);
        }
        L("debug", "musicDetailHeaderRenderer cover_url", headerInfo.cover_url);
      }
      // musicImmersiveHeaderRenderer for some playlists
      if (header.musicImmersiveHeaderRenderer) {
        var ih = header.musicImmersiveHeaderRenderer;
        if (ih.title && ih.title.runs) {
          headerInfo.name = ih.title.runs.map(function(r){ return r.text; }).join("");
        }
        if (ih.description && ih.description.runs) {
          headerInfo.artists = ih.description.runs.map(function(r){ return r.text; }).join("");
        }
        if (ih.thumbnail && ih.thumbnail.musicThumbnailRenderer && ih.thumbnail.musicThumbnailRenderer.thumbnail) {
          var thumbUrl2 = pickLastThumbnailUrl(ih.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails);
          headerInfo.cover_url = makeSquareThumb(thumbUrl2);
        }
        headerInfo.album_type = "playlist";
        L("debug", "musicImmersiveHeaderRenderer cover_url", headerInfo.cover_url);
      }
      // musicVisualHeaderRenderer (another variant)
      if (header.musicVisualHeaderRenderer) {
        var vh = header.musicVisualHeaderRenderer;
        if (vh.title && vh.title.runs) {
          headerInfo.name = vh.title.runs.map(function(r){ return r.text; }).join("");
        }
        if (vh.foregroundThumbnail && vh.foregroundThumbnail.musicThumbnailRenderer && vh.foregroundThumbnail.musicThumbnailRenderer.thumbnail) {
          var thumbUrl3 = pickLastThumbnailUrl(vh.foregroundThumbnail.musicThumbnailRenderer.thumbnail.thumbnails);
          headerInfo.cover_url = makeSquareThumb(thumbUrl3);
        }
        L("debug", "musicVisualHeaderRenderer cover_url", headerInfo.cover_url);
      }
    }
    
    // Fallback: try to get cover from first track's thumbnail if header has no cover
    L("debug", "parseBrowseResponse headerInfo.cover_url after header parse", headerInfo.cover_url);
    
    // Collect tracks from content - use collectAlbumTracksOnly to avoid "Related" sections
    var trackCandidates = [];
    if (data.contents) {
      collectAlbumTracksOnly(data.contents, trackCandidates);
    }
    
    // Fallback to old method if new method finds nothing
    if (trackCandidates.length === 0 && data.contents) {
      L("debug", "parseBrowseResponse: collectAlbumTracksOnly found nothing, falling back to collectItemsFromNode");
      collectItemsFromNode(data.contents, trackCandidates, 0);
    }
    
    L("debug", "parseBrowseResponse found candidates", trackCandidates.length);
    
    // Parse each track
    var tracks = [];
    for (var i = 0; i < trackCandidates.length; i++) {
      var node = trackCandidates[i];
      var possible = node.musicResponsiveListItemRenderer || node.playlistPanelVideoRenderer || node;
      var parsed = parseItemExtended(possible);
      if (parsed) {
        // Add album info to track
        parsed.album = headerInfo.name;
        // Fallback to album artist if track artist is empty
        if (!parsed.artist && headerInfo.artists) {
          parsed.artist = headerInfo.artists;
        }
        var sanitized = sanitizeTrackBeforeReturn(parsed);
        if (sanitized) {
          sanitized.album_name = headerInfo.name;
          // Also fallback artists in sanitized result
          if (!sanitized.artists && headerInfo.artists) {
            sanitized.artists = headerInfo.artists;
          }
          tracks.push(sanitized);
        }
      }
    }
    
    L("info", "parseBrowseResponse parsed tracks", tracks.length);
    
    headerInfo.total_tracks = tracks.length;
    
    // Fallback: if no cover from header, try to get from first track
    if (!headerInfo.cover_url && tracks.length > 0 && tracks[0].cover_url) {
      headerInfo.cover_url = tracks[0].cover_url;
      L("debug", "parseBrowseResponse using first track cover as fallback", headerInfo.cover_url);
    }
    
    // Determine type based on browseId prefix
    if (browseId.startsWith("VL") || browseId.startsWith("PL") || browseId.startsWith("RDCLAK5uy_")) {
      headerInfo.album_type = "playlist";
    } else if (browseId.startsWith("MPREb_")) {
      // Keep detected type from subtitle, default to album
      if (!headerInfo.album_type) headerInfo.album_type = "album";
    }
    
    L("debug", "parseBrowseResponse final cover_url", headerInfo.cover_url);
    
    return {
      type: headerInfo.album_type === "playlist" ? "playlist" : "album",
      album: headerInfo,
      tracks: tracks,
      cover_url: headerInfo.cover_url,
      name: headerInfo.name
    };
  } catch (e) {
    L("error", "parseBrowseResponse error", String(e));
    return null;
  }
}

// Fetch video metadata by ID
async function fetchVideoMetadata(videoId) {
  var url = "https://music.youtube.com/youtubei/v1/player?alt=json";
  var body = JSON.stringify({
    context: { client: { clientName: "WEB_REMIX", clientVersion: CONFIG.clientVersion } },
    videoId: videoId
  });
  
  var res = await safeFetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": "Mozilla/5.0 (compatible)",
      "x-youtube-client-name": "WEB_REMIX",
      "x-youtube-client-version": CONFIG.clientVersion
    },
    body: body
  });
  
  var data = await res.json();
  if (!data || !data.videoDetails) return null;
  
  var details = data.videoDetails;
  var thumb = null;
  if (details.thumbnail && details.thumbnail.thumbnails && details.thumbnail.thumbnails.length > 0) {
    var lastThumb = details.thumbnail.thumbnails[details.thumbnail.thumbnails.length - 1];
    thumb = makeSquareThumb(lastThumb.url);
  }
  
  return {
    id: videoId,
    title: details.title || "Unknown",
    artist: details.author || "",
    album: "",
    duration: parseInt(details.lengthSeconds, 10) || 0,
    thumbnail: thumb,
    source: "youtube"
  };
}

// Handle URL function for URL handler
function handleUrl(url) {
  L("info", "handleUrl called", url);
  
  // First check for browse ID (album/playlist page)
  var browseId = extractBrowseId(url);
  if (browseId) {
    L("info", "handleUrl: detected browseId", browseId);
    var browseResult = fetchBrowseTracksSync(browseId);
    if (browseResult) {
      return browseResult;
    }
  }
  
  // Check for playlist ID
  var playlistId = extractPlaylistId(url);
  if (playlistId && !browseId) {
    L("info", "handleUrl: detected playlistId", playlistId);
    // Playlist IDs need VL prefix for browse API
    var browsePL = playlistId.startsWith("VL") ? playlistId : "VL" + playlistId;
    var playlistResult = fetchBrowseTracksSync(browsePL);
    if (playlistResult) {
      return playlistResult;
    }
  }
  
  // Check for video ID (single track)
  var videoId = extractVideoId(url);
  if (!videoId) {
    L("warn", "handleUrl: no video ID found", url);
    return null;
  }
  
  var key = "yt:video:" + videoId;
  var cached = cacheGet(key);
  if (cached) {
    L("info", "handleUrl: returning cached", videoId);
    return { type: "track", track: cached };
  }
  
  // Start async fetch
  dedupFetch(key, async function() {
    try {
      var track = await fetchVideoMetadata(videoId);
      if (track) {
        var sanitized = sanitizeTrackBeforeReturn(track);
        if (sanitized) {
          cacheSet(key, sanitized);
          L("info", "handleUrl: cached video metadata", videoId);
        }
      }
    } catch (e) {
      L("error", "handleUrl fetch failed", String(e));
    }
  }).catch(function(){});
  
  // Return placeholder while loading
  return {
    type: "track",
    track: {
      id: videoId,
      title: "Loading...",
      artist: "",
      album: "",
      duration: 0,
      thumbnail: null,
      source: "youtube"
    }
  };
}

// Get album tracks by browseId
function getAlbum(albumId) {
  L("info", "getAlbum called", albumId);
  try {
    var result = fetchBrowseTracksSync(albumId);
    if (result && result.tracks) {
      return {
        id: albumId,
        name: result.name || "",
        artists: result.album ? result.album.artists : "",
        cover_url: result.cover_url,
        release_date: result.album ? result.album.release_date : "",
        total_tracks: result.tracks.length,
        album_type: result.album ? result.album.album_type : "album",
        tracks: result.tracks,
        provider_id: "ytmusic-spotiflac"
      };
    }
    return null;
  } catch (e) {
    L("error", "getAlbum error", String(e));
    return null;
  }
}

// Get playlist tracks by playlistId
function getPlaylist(playlistId) {
  L("info", "getPlaylist called", playlistId);
  try {
    // Playlist IDs need VL prefix for browse API if not present
    var browseId = playlistId;
    if (!playlistId.startsWith("VL") && !playlistId.startsWith("RDCLAK5uy_")) {
      browseId = "VL" + playlistId;
    }
    
    var result = fetchBrowseTracksSync(browseId);
    if (result && result.tracks) {
      return {
        id: playlistId,
        name: result.name || "",
        owner: result.album ? result.album.artists : "",
        cover_url: result.cover_url,
        total_tracks: result.tracks.length,
        tracks: result.tracks,
        provider_id: "ytmusic-spotiflac"
      };
    }
    return null;
  } catch (e) {
    L("error", "getPlaylist error", String(e));
    return null;
  }
}

// Get artist info and albums by artistId (channel ID starting with UC)
function getArtist(artistId) {
  L("info", "getArtist called", artistId);
  try {
    // Fetch artist page
    var url = "https://music.youtube.com/youtubei/v1/browse?alt=json";
    var body = JSON.stringify({
      context: { client: { clientName: "WEB_REMIX", clientVersion: CONFIG.clientVersion } },
      browseId: artistId
    });
    
    var res = fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": getRandomUserAgent(),
        "x-youtube-client-name": "WEB_REMIX",
        "x-youtube-client-version": CONFIG.clientVersion
      },
      body: body
    });
    
    if (!res || !res.ok) {
      L("error", "getArtist fetch failed", res ? res.status : "no response");
      return null;
    }
    
    var data = res.json();
    if (!data) {
      L("error", "getArtist json parse failed");
      return null;
    }
    
    // Extract artist info from header
    var artistName = "";
    var artistImage = null;
    
    if (data.header) {
      var header = data.header.musicImmersiveHeaderRenderer || data.header.musicVisualHeaderRenderer || data.header.musicDetailHeaderRenderer;
      if (header) {
        if (header.title && header.title.runs) {
          artistName = header.title.runs.map(function(r){ return r.text; }).join("");
        }
        // Get thumbnail
        if (header.thumbnail && header.thumbnail.musicThumbnailRenderer && header.thumbnail.musicThumbnailRenderer.thumbnail) {
          var thumbUrl = pickLastThumbnailUrl(header.thumbnail.musicThumbnailRenderer.thumbnail.thumbnails);
          artistImage = makeSquareThumb(thumbUrl);
        }
        if (!artistImage && header.foregroundThumbnail && header.foregroundThumbnail.musicThumbnailRenderer) {
          var thumbUrl2 = pickLastThumbnailUrl(header.foregroundThumbnail.musicThumbnailRenderer.thumbnail.thumbnails);
          artistImage = makeSquareThumb(thumbUrl2);
        }
      }
    }
    
    // Collect albums from content
    var albums = [];
    if (data.contents) {
      var candidates = [];
      collectItemsFromNode(data.contents, candidates, 0);
      
      for (var i = 0; i < candidates.length; i++) {
        var node = candidates[i];
        var parsed = parseCollectionItem(node);
        if (parsed && (parsed.item_type === "album" || parsed.item_type === "playlist")) {
          var sanitized = sanitizeCollectionBeforeReturn(parsed);
          if (sanitized) {
            albums.push({
              id: sanitized.id,
              name: sanitized.name,
              artists: sanitized.artists || artistName,
              cover_url: sanitized.cover_url,
              release_date: sanitized.release_date || "",
              total_tracks: 0,
              album_type: sanitized.album_type || "album",
              provider_id: "ytmusic-spotiflac"
            });
          }
        }
      }
    }
    
    L("info", "getArtist found albums", albums.length);
    
    return {
      id: artistId,
      name: artistName,
      image_url: artistImage,
      albums: albums,
      provider_id: "ytmusic-spotiflac"
    };
  } catch (e) {
    L("error", "getArtist error", String(e));
    return null;
  }
}

// Enrich track with ISRC and external service links via Odesli (song.link) API
function enrichTrack(track) {
  L("info", "enrichTrack called", track ? track.id : "null");
  
  if (!track || !track.id) {
    L("warn", "enrichTrack: invalid track");
    return track;
  }
  
  // Build YouTube Music URL for Odesli lookup
  var ytUrl = "https://music.youtube.com/watch?v=" + encodeURIComponent(track.id);
  var odesliUrl = "https://api.song.link/v1-alpha.1/links?url=" + encodeURIComponent(ytUrl);
  
  var cacheKey = "odesli:" + track.id;
  var cached = cacheGet(cacheKey);
  if (cached) {
    L("info", "enrichTrack: returning cached enrichment", track.id);
    return Object.assign({}, track, cached);
  }
  
  try {
    var res = fetch(odesliUrl, {
      method: "GET",
      headers: {
        "User-Agent": getRandomUserAgent()
      }
    });
    
    if (!res || !res.ok) {
      L("warn", "enrichTrack: Odesli API returned status", res ? res.status : "null");
      return track;
    }
    
    var data = res.json();
    if (!data) {
      L("warn", "enrichTrack: failed to parse Odesli response");
      return track;
    }
    
    L("debug", "enrichTrack: Odesli response keys", Object.keys(data));
    
    var enrichment = {};
    
    // Extract Deezer URL first (we'll need it for ISRC fallback)
    var deezerUrl = null;
    var deezerTrackId = null;
    if (data.linksByPlatform && data.linksByPlatform.deezer && data.linksByPlatform.deezer.url) {
      deezerUrl = data.linksByPlatform.deezer.url;
      var deezerMatch = deezerUrl.match(/\/track\/(\d+)/);
      if (deezerMatch) {
        deezerTrackId = deezerMatch[1];
      }
      L("debug", "enrichTrack: Got Deezer URL from Odesli", deezerUrl);
    }
    
    // Extract ISRC from entitiesByUniqueId
    if (data.entitiesByUniqueId) {
      var entities = data.entitiesByUniqueId;
      var entityKeys = Object.keys(entities);
      
      for (var i = 0; i < entityKeys.length; i++) {
        var entity = entities[entityKeys[i]];
        if (entity && entity.isrc && !enrichment.isrc) {
          enrichment.isrc = entity.isrc;
          L("info", "enrichTrack: found ISRC from Odesli entities", enrichment.isrc);
        }
        // Also extract additional metadata if available
        if (entity && entity.title && !enrichment.enriched_title) {
          enrichment.enriched_title = entity.title;
        }
        if (entity && entity.artistName && !enrichment.enriched_artist) {
          enrichment.enriched_artist = entity.artistName;
        }
      }
    }
    
    // Fallback: Get ISRC from Deezer API if not found in Odesli entities
    if (!enrichment.isrc && deezerTrackId) {
      L("debug", "enrichTrack: ISRC not in Odesli, fetching from Deezer API...");
      try {
        var deezerApiUrl = "https://api.deezer.com/track/" + deezerTrackId;
        var deezerRes = fetch(deezerApiUrl, {
          method: "GET",
          headers: {
            "User-Agent": getRandomUserAgent()
          }
        });
        
        if (deezerRes && deezerRes.ok) {
          var deezerData = deezerRes.json();
          if (deezerData && deezerData.isrc) {
            enrichment.isrc = deezerData.isrc;
            L("info", "enrichTrack: Got ISRC from Deezer API", enrichment.isrc);
          }
        } else {
          L("debug", "enrichTrack: Deezer API failed", deezerRes ? deezerRes.status : "null");
        }
      } catch (deezerErr) {
        L("debug", "enrichTrack: Deezer API error", String(deezerErr));
      }
    }
    
    // Extract links to other services
    if (data.linksByPlatform) {
      var links = data.linksByPlatform;
      enrichment.external_links = {};
      L("debug", "enrichTrack: linksByPlatform keys", Object.keys(links));
      
      // Priority services for SpotiFLAC fallback: Deezer, Tidal, Qobuz, Spotify
      // Use already extracted deezerUrl and deezerTrackId from above
      if (deezerUrl) {
        enrichment.external_links.deezer = deezerUrl;
        if (deezerTrackId) {
          enrichment.deezer_id = deezerTrackId;
        }
      }
      if (links.tidal && links.tidal.url) {
        enrichment.external_links.tidal = links.tidal.url;
        // Extract Tidal track ID if available
        var tidalMatch = links.tidal.url.match(/\/track\/(\d+)/);
        if (tidalMatch) {
          enrichment.tidal_id = tidalMatch[1];
        }
      }
      if (links.qobuz && links.qobuz.url) {
        enrichment.external_links.qobuz = links.qobuz.url;
        // Extract Qobuz track ID if available (format: /track/123456789)
        var qobuzMatch = links.qobuz.url.match(/\/track\/(\d+)/);
        if (qobuzMatch) {
          enrichment.qobuz_id = qobuzMatch[1];
        }
      }
      if (links.spotify && links.spotify.url) {
        enrichment.external_links.spotify = links.spotify.url;
        // Extract Spotify track ID if available
        var spotifyMatch = links.spotify.url.match(/\/track\/([a-zA-Z0-9]+)/);
        if (spotifyMatch) {
          enrichment.spotify_id = spotifyMatch[1];
        }
      }
      if (links.amazonMusic && links.amazonMusic.url) {
        enrichment.external_links.amazon = links.amazonMusic.url;
      }
      if (links.appleMusic && links.appleMusic.url) {
        enrichment.external_links.apple = links.appleMusic.url;
      }
      
      L("info", "enrichTrack: found external links", Object.keys(enrichment.external_links));
      if (enrichment.tidal_id) L("info", "enrichTrack: tidal_id extracted", enrichment.tidal_id);
      if (enrichment.qobuz_id) L("info", "enrichTrack: qobuz_id extracted", enrichment.qobuz_id);
      if (enrichment.deezer_id) L("info", "enrichTrack: deezer_id extracted", enrichment.deezer_id);
      if (enrichment.spotify_id) L("info", "enrichTrack: spotify_id extracted", enrichment.spotify_id);
    }
    
    // Cache the enrichment data
    if (enrichment.isrc || (enrichment.external_links && Object.keys(enrichment.external_links).length > 0)) {
      cacheSet(cacheKey, enrichment);
    }
    
    // Merge enrichment with original track
    var enrichedTrack = Object.assign({}, track, enrichment);
    L("info", "enrichTrack: success", { id: track.id, hasIsrc: !!enrichment.isrc, linkCount: enrichment.external_links ? Object.keys(enrichment.external_links).length : 0 });
    
    return enrichedTrack;
    
  } catch (e) {
    L("error", "enrichTrack: Odesli API error", String(e));
    return track;
  }
}

registerExtension({
  initialize: function() { L("info", "YouTube Music extension init"); return true; },
  customSearch: function(query) {
    L("info", "customSearch", query);
    try {
      return customSearchSync(query);
    } catch (e) {
      L("error", "customSearch fatal", String(e));
      return [];
    }
  },
  handleUrl: handleUrl,
  getAlbum: getAlbum,
  getPlaylist: getPlaylist,
  getArtist: getArtist,
  enrichTrack: enrichTrack,
  validateTrackForDownload: validateTrackForDownload,
  finalGuardBeforeNative: finalGuardBeforeNative,
  matchTrack: function() { return null; },
  checkAvailability: function() { return false; },
  getDownloadUrl: function() { return null; },
  download: function() { return null; },
  cleanup: function() { L("info", "YouTube Music extension cleanup"); return true; }
});