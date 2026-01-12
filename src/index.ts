import "./polyfill";
import { Innertube, UniversalCache } from "youtubei.js";

const DEBUG = true;

function logDebug(...args: any[]) {
  if (DEBUG) console.log("[YTMusic][DEBUG]", ...args);
}

function logError(...args: any[]) {
  console.log("[YTMusic][ERROR]", ...args);
}

let yt: Innertube | null = null;
let initPromise: Promise<void> | null = null;

// Host-provided function (SpotiFLAC)
declare function registerExtension(cb: (extension: any) => void): void;

// ---------------------------------------------------------
// INIT
// ---------------------------------------------------------
async function initialize() {
  if (yt) return;
  logDebug("Initializing Innertube...");
  yt = await Innertube.create({
    cache: new UniversalCache(false),
    generate_session_locally: true
  });
  logDebug("Innertube initialized");
}

// ---------------------------------------------------------
// URL PARSER
// ---------------------------------------------------------
function isYoutubeUrl(input: any): boolean {
  if (typeof input !== "string") return false;
  return (
    input.includes("youtube.com/") ||
    input.includes("youtu.be/") ||
    input.includes("music.youtube.com/")
  );
}

function extractVideoIdFromUrl(url: string): string | null {
  try {
    const u = new URL(url);
    if (u.hostname === "youtu.be") return u.pathname.replace("/", "");
    const v = u.searchParams.get("v");
    return v || null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------
// TRACK MATCHING SCORING
// ---------------------------------------------------------
function scoreCandidate(item: any, ctx: any): number {
  if (!ctx) return 0;

  let score = 0;
  const t = (s: string) => (s || "").toLowerCase();

  const candTitle = t(item.title);
  const candArtist = t(item.artist);
  const candAlbum = t(item.album);

  const ctxTitle = t(ctx.title);
  const ctxArtist = t(ctx.artist);
  const ctxAlbum = t(ctx.album);

  const candDur = item.durationMs || item.duration || 0;
  const ctxDur = ctx.duration || 0;

  if (candTitle === ctxTitle) score += 40;
  else if (candTitle.includes(ctxTitle) || ctxTitle.includes(candTitle)) score += 25;

  if (candArtist === ctxArtist) score += 30;
  else if (candArtist.includes(ctxArtist) || ctxArtist.includes(candArtist)) score += 20;

  if (candAlbum === ctxAlbum) score += 15;
  else if (candAlbum.includes(ctxAlbum) || ctxAlbum.includes(candAlbum)) score += 8;

  if (ctxDur > 0 && candDur > 0) {
    const diff = Math.abs(candDur - ctxDur);
    if (diff <= 2000) score += 15;
    else if (diff <= 5000) score += 8;
  }

  return score;
}

// ---------------------------------------------------------
// ALBUM / SINGLE DETECTION
// ---------------------------------------------------------
function detectAlbumType(item: any): string {
  const name = (item.album?.name || item.album || "").toLowerCase();
  const type = (item.album?.type || "").toLowerCase();
  if (type === "single") return "single";
  if (name.includes("single")) return "single";
  return "album";
}

// ---------------------------------------------------------
// QUALITY SELECTION (uses user-selected quality)
// ---------------------------------------------------------
function selectBestAudioStream(sd: any, quality: string): any | null {
  if (!sd) return null;

  const adaptive = sd.adaptive_formats || sd.adaptiveFormats || [];
  let audio = adaptive.filter((f: any) =>
    (f.mime_type || f.mimeType || "").includes("audio")
  );

  if (!audio.length) {
    const formats = sd.formats || [];
    audio = formats.filter((f: any) =>
      (f.mime_type || f.mimeType || "").includes("audio")
    );
    if (!audio.length) return null;
  }

  const norm = (f: any) => ({
    ...f,
    mime: f.mime_type || f.mimeType || "",
    bitrate: f.bitrate || 0
  });

  const list = audio.map(norm);

  // User-selected quality
  if (quality === "AAC_256") {
    const aac256 = list.find((f) => f.mime.includes("mp4a") && f.bitrate >= 230000);
    if (aac256) return aac256;
  }

  if (quality === "OPUS_160") {
    const opus160 = list.find(
      (f) => f.mime.includes("opus") && f.bitrate >= 150000 && f.bitrate <= 190000
    );
    if (opus160) return opus160;
  }

  if (quality === "AAC_128") {
    const aac128 = list.find(
      (f) => f.mime.includes("mp4a") && f.bitrate >= 110000 && f.bitrate <= 150000
    );
    if (aac128) return aac128;
  }

  // Fallback: highest bitrate
  return list.sort((a, b) => b.bitrate - a.bitrate)[0];
}

// ---------------------------------------------------------
// SEARCH (Custom Search enabled)
// ---------------------------------------------------------
async function search(entry: any) {
  logDebug("YTMusic.search() called with:", entry);

  if (!yt) {
    if (!initPromise) initPromise = initialize();
    await initPromise;
  }

  // Direct URL
  if (typeof entry === "string" && isYoutubeUrl(entry)) {
    const id = extractVideoIdFromUrl(entry);
    if (!id) return [];

    try {
      const info = await yt!.music.getInfo(id);
      return [
        {
          id,
          title: info.basic_info?.title || "Unknown",
          artist:
            info.basic_info?.author ||
            info.basic_info?.artists?.[0]?.name ||
            "Unknown",
          album: info.basic_info?.album?.name || "Unknown",
          duration: info.basic_info?.duration || 0,
          source: "youtube_music",
          url: `https://music.youtube.com/watch?v=${id}`,
          is_playable: true,
          thumbnail: info.basic_info?.thumbnail?.[0]?.url || null,
          album_type: "single"
        }
      ];
    } catch (e) {
      logError("URL lookup failed:", e);
      return [];
    }
  }

  // Track object
  let query = entry;
  let ctx: any = null;

  if (entry && typeof entry === "object" && entry.title) {
    ctx = {
      title: entry.title,
      artist: entry.artist || entry.artists || "",
      album: entry.album || "",
      duration: entry.duration || 0
    };
    query = `${ctx.artist} - ${ctx.title}`;
  }

  if (typeof query !== "string" || !query.trim()) return [];

  try {
    const result = await yt!.music.search(query);
    if (!result?.songs?.contents) return [];

    let mapped = result.songs.contents.map((item: any) => {
      const dur = item.duration?.seconds ? item.duration.seconds * 1000 : 0;
      return {
        id: item.id,
        title: item.title,
        artist: item.artists?.map((a: any) => a.name).join(", ") || "Unknown",
        album: item.album?.name || "Unknown",
        duration: dur,
        durationMs: dur,
        source: "youtube_music",
        url: `https://music.youtube.com/watch?v=${item.id}`,
        is_playable: true,
        thumbnail: item.thumbnail?.[0]?.url || null,
        album_type: detectAlbumType(item)
      };
    });

    if (ctx) {
      mapped = mapped
        .map((i) => ({ ...i, match_score: scoreCandidate(i, ctx) }))
        .sort((a, b) => (b.match_score || 0) - (a.match_score || 0));
    }

    return mapped;
  } catch (e) {
    logError("Search failed:", e);
    return [];
  }
}

// ---------------------------------------------------------
// DOWNLOAD (Quality-aware)
// ---------------------------------------------------------
async function getDownloadInfo(id: string, extension?: any) {
  logDebug("YTMusic.getDownloadInfo() called with id:", id);

  if (!yt) {
    if (!initPromise) initPromise = initialize();
    await initPromise;
  }

  const selectedQuality = extension?.quality?.id || "AAC_256";
  logDebug("Selected quality:", selectedQuality);

  try {
    const info = await yt!.music.getInfo(id);
    const sd = info.streaming_data || info.streamingData;

    const best = selectBestAudioStream(sd, selectedQuality);
    if (!best) {
      logError("No audio stream found");
      return null;
    }

    return {
      url: best.url,
      mime: best.mime,
      bitrate: best.bitrate,
      codec: best.codec || null
    };
  } catch (e) {
    logError("Download info failed:", e);
    return null;
  }
}

// ---------------------------------------------------------
// HOST REGISTRATION
// ---------------------------------------------------------
registerExtension(function (extension: any) {
  try {
    extension.registerSearchProvider(search);
    extension.registerDownloadProvider((id: string) =>
      getDownloadInfo(id, extension)
    );
    console.log("[YTMusic] registerExtension() completed");
  } catch (e) {
    console.log("[YTMusic] registerExtension() error:", e);
  }
});
