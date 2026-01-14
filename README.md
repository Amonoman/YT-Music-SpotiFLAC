# YT‑Music‑SpotiFLAC

<img src="icon-proj.png" width="256" />

A compact, high‑performance SpotiFLAC extension that integrates YouTube Music as a metadata and search source. It provides reliable search results, robust metadata matching, and is designed as a metadata‑only provider so SpotiFLAC can use other providers for downloads.

---

Features
- Accurate search with multi‑stage query handling and YouTube Music compatibility  
- Advanced metadata matching using title, artist, album and duration scoring  
- Metadata only — no audio URLs or direct downloads supplied by this extension  
- Performance optimizations: caching, simple rate‑limit backoff, and lightweight parsing  
- SpotiFLAC 3.x compatible: packaged as a .spotiflac-ext with index.js and main.js at the archive root

---

Installation
1. Download the latest YT-Music-SpotiFLAC.spotiflac-ext from Releases.  
2. Open SpotiFLAC and go to Extensions.  
3. Import the .spotiflac-ext file.  
4. Enable the extension and Enjoy.

---

Development
- Install dependencies
`bash
npm install
`
- Build
`bash
npm run build
`
- Build output
  - The build produces index.js and main.js at the package root and the src/ modules used by the extension.  
  - Package the extension as YT-Music-SpotiFLAC.spotiflac-ext (ZIP) including index.js, main.js, manifest.json, icons/, and src/.

---

Configuration
- manifest.json includes required fields such as name, displayName, version, description, author, main, icon, permissions and type.  
- Permissions should be minimal and explicit, for example:
`json
"permissions": {
  "network": ["music.youtube.com", "*.youtube.com"],
  "storage": true
}
`
- Icons
  - icons/icon.png — extension icon (recommended 128×128, squircle)  
  - icons/icon-proj.png — project icon for README and GitHub

---

License
This project is licensed under the GNU General Public License v3.0 (GPL‑3.0). You may use, modify, and distribute this software under the GPL‑3.0 terms. Any derivative works that include this code must also be released under GPL‑3.0 and provide corresponding source code.

For the full license text consult the official GNU documentation.
