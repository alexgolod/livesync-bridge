import { join as joinPosix } from "jsr:@std/path/posix";
import { resolve } from "jsr:@std/path";
import type { FileInfo } from "./lib/src/API/DirectFileManipulatorV2.ts";

import { FilePathWithPrefix, LOG_LEVEL, LOG_LEVEL_DEBUG, LOG_LEVEL_INFO } from "./lib/src/common/types.ts";
import { PeerConf, FileData } from "./types.ts";
import { Logger } from "octagonal-wheels/common/logger.js";
import { LRUCache } from "octagonal-wheels/memory/LRUCache.js"
import { computeHash } from "./util.ts";

// File-based settings persistence (survives SIGTERM, no localStorage scoping issues).
// Directory created lazily on first write to avoid module-level side effects.
const SETTINGS_DIR = resolve(new URL(".", import.meta.url).pathname, "dat", "settings");
let settingsDirCreated = false;

export type DispatchFun = (source: Peer, path: string, data: FileData | false) => Promise<void>;

export abstract class Peer {
    config: PeerConf;
    // hub: Hub;
    dispatchToHub: DispatchFun;
    constructor(conf: PeerConf, dispatcher: DispatchFun) {
        this.config = conf;
        this.dispatchToHub = dispatcher;
    }
    toLocalPath(path: string) {
        const relativeJoined = joinPosix(this.config.baseDir, path);
        const relative = relativeJoined == "." ? "" : relativeJoined;
        const ret = (relative.startsWith("_")) ? ("/" + relative) : relative;
        // this.debugLog(`**TOLOCAL: ${path} => ${ret}`);
        return ret;
    }
    toGlobalPath(pathSrc: string) {
        let path = pathSrc.startsWith("_") ? pathSrc.substring(1) : pathSrc;
        if (path.startsWith(this.config.baseDir)) {
            path = path.substring(this.config.baseDir.length);
        }
        // this.debugLog(`**TOLOCAL: ${pathSrc} => ${path}`);
        return path;
    }
    abstract delete(path: string): Promise<boolean>;
    abstract put(path: string, data: FileData): Promise<boolean>;
    abstract get(path: FilePathWithPrefix): Promise<false | FileData>;
    abstract start(): Promise<void>;
    abstract stop(): Promise<void>;
    cache = new LRUCache<string, string>(300, 10000000, true);
    async isRepeating(path: string, data: FileData | false) {
        const d = await computeHash(data === false ? ["\u0001Deleted"] : data.data);

        if (this.cache.has(path) && this.cache.get(path) == d) {
            return true;
        }
        this.cache.set(path, d);
        return false;
    }
    receiveLog(message: string, level?: LOG_LEVEL) {
        Logger(`[${this.config.name}] <-- ${message}`, level ?? LOG_LEVEL_INFO);
    }
    sendLog(message: string, level?: LOG_LEVEL) {
        Logger(`[${this.config.name}] --> ${message}`, level ?? LOG_LEVEL_INFO);
    }
    normalLog(message: string, level?: LOG_LEVEL) {
        Logger(`[${this.config.name}] ${message}`, level ?? LOG_LEVEL_INFO);
    }
    debugLog(message: string, level?: LOG_LEVEL) {
        Logger(`[${this.config.name}] ${message}`, level ?? LOG_LEVEL_DEBUG);
    }
    _getKey(key: string) {
        return `${this.config.name}-${this.config.type}-${this.config.baseDir}-${key}`;
    }
    _settingsFile(): string {
        const safe = this._getKey("").replace(/[^a-zA-Z0-9_-]/g, "_");
        return resolve(SETTINGS_DIR, `${safe}.json`);
    }
    _fileSettings?: Record<string, string>;
    _loadFileSettings(): Record<string, string> {
        if (this._fileSettings) return this._fileSettings;
        try {
            this._fileSettings = JSON.parse(Deno.readTextFileSync(this._settingsFile()));
        } catch {
            this._fileSettings = {};
        }
        return this._fileSettings;
    }
    _saveFileSettings() {
        if (!settingsDirCreated) {
            try { Deno.mkdirSync(SETTINGS_DIR, { recursive: true }); } catch { /* exists */ }
            settingsDirCreated = true;
        }
        Deno.writeTextFileSync(this._settingsFile(), JSON.stringify(this._fileSettings ?? {}));
    }
    setSetting(key: string, value: string) {
        // Write to both localStorage (for upstream compat) and file (reliable)
        localStorage.setItem(this._getKey(key), value);
        const s = this._loadFileSettings();
        s[key] = value;
        this._saveFileSettings();
    }
    getSetting(key: string) {
        // Prefer file-based, fall back to localStorage
        const s = this._loadFileSettings();
        if (key in s) return s[key];
        return localStorage.getItem(this._getKey(key));
    }
    compareDate(a: FileInfo, b: FileInfo) {
        const aMTime = ~~(a?.mtime ?? 0 / 1000);
        const bMTime = ~~(b?.mtime ?? 0 / 1000);
        return aMTime - bMTime;
    }
}
