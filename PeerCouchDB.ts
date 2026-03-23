import { DirectFileManipulator, FileInfo, MetaEntry, ReadyEntry } from "./lib/src/API/DirectFileManipulatorV2.ts";
import { FilePathWithPrefix, LOG_LEVEL_NOTICE, MILESTONE_DOCID, TweakValues } from "./lib/src/common/types.ts";
import { PeerCouchDBConf, FileData } from "./types.ts";
import { decodeBinary } from "./lib/src/string_and_binary/convert.ts";
import { isPlainText } from "./lib/src/string_and_binary/path.ts";
import { DispatchFun, Peer } from "./Peer.ts";
import { createBinaryBlob, createTextBlob, isDocContentSame, unique } from "./lib/src/common/utils.ts";

// export class PeerInstance()

export class PeerCouchDB extends Peer {
    man: DirectFileManipulator;
    declare config: PeerCouchDBConf;
    watchdogInterval?: ReturnType<typeof setInterval>;
    lastEventTime = Date.now();
    consecutiveReconnects = 0;
    sinceDebounceTimer?: ReturnType<typeof setTimeout>;

    static WATCHDOG_CHECK_MS = 60_000;       // check every 60s
    static WATCHDOG_STALE_MS = 5 * 60_000;   // stale after 5 min of silence
    static MAX_BACKOFF_MS = 10 * 60_000;     // max 10 min between reconnects

    constructor(conf: PeerCouchDBConf, dispatcher: DispatchFun) {
        super(conf, dispatcher);
        this.man = new DirectFileManipulator(conf);
        // Fetch remote since.
        this.man.since = this.getSetting("since") || "now";
    }
    async delete(pathSrc: string): Promise<boolean> {
        const path = this.toLocalPath(pathSrc);
        if (await this.isRepeating(pathSrc, false)) {
            return false;
        }
        const r = await this.man.delete(path);
        if (r) {
            this.receiveLog(` ${path} deleted`);
        } else {
            this.receiveLog(` ${path} delete failed`, LOG_LEVEL_NOTICE);
        }
        return r;
    }
    async put(pathSrc: string, data: FileData): Promise<boolean> {
        const path = this.toLocalPath(pathSrc);
        if (await this.isRepeating(pathSrc, data)) {
            return false;
        }
        const type = isPlainText(path) ? "plain" : "newnote";
        const info: FileInfo = {
            ctime: data.ctime,
            mtime: data.mtime,
            size: data.size
        };
        const saveData = (data.data instanceof Uint8Array) ? createBinaryBlob(data.data) : createTextBlob(data.data);
        const old = await this.man.get(path as FilePathWithPrefix, true) as false | MetaEntry;
        // const old = await this.getMeta(path as FilePathWithPrefix);
        if (old && Math.abs(this.compareDate(info, old)) < 3600) {
            const oldDoc = await this.man.getByMeta(old);
            if (oldDoc && ("data" in oldDoc)) {
                const d = oldDoc.type == "plain" ? createTextBlob(oldDoc.data) : createBinaryBlob(new Uint8Array(decodeBinary(oldDoc.data)));
                if (await isDocContentSame(d, saveData)) {
                    this.normalLog(` Skipped (Same) ${path} `);
                    return false;
                }
            }
        }
        const r = await this.man.put(path, saveData, info, type);
        if (r) {
            this.receiveLog(` ${path} saved`);
        } else {
            this.receiveLog(` ${path} ignored`);
        }
        return r;
    }
    async get(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path) as false | ReadyEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: ret.type == "newnote" ? new Uint8Array(decodeBinary(ret.data)) : ret.data,
            size: ret.size,
            deleted: ret.deleted
        };
    }
    async getMeta(pathSrc: FilePathWithPrefix): Promise<false | FileData> {
        const path = this.toLocalPath(pathSrc) as FilePathWithPrefix;
        const ret = await this.man.get(path, true) as false | MetaEntry;
        if (ret === false) {
            return false;
        }
        return {
            ctime: ret.ctime,
            mtime: ret.mtime,
            data: [],
            size: ret.size,
            deleted: ret.deleted
        };
    }

    // Debounced since persistence — update in-memory immediately, write to disk every 2s
    _persistSince(seq: string) {
        this.man.since = seq;
        if (this.sinceDebounceTimer) clearTimeout(this.sinceDebounceTimer);
        this.sinceDebounceTimer = setTimeout(() => {
            this.setSetting("since", seq);
        }, 2000);
    }

    _flushSince() {
        if (this.sinceDebounceTimer) {
            clearTimeout(this.sinceDebounceTimer);
            this.sinceDebounceTimer = undefined;
        }
        if (this.man.since) {
            this.setSetting("since", `${this.man.since}`);
        }
    }

    // Build the _changes feed callback (processes each interesting change)
    _makeChangeCallback(baseDir: string) {
        return async (entry: ReadyEntry, seq?: string | number) => {
            // Track since from the sequence number for reliable reconnects
            if (seq !== undefined) {
                this._persistSince(`${seq}`);
            }
            this.consecutiveReconnects = 0; // got a real event, reset backoff
            const d = entry.type == "plain" ? entry.data : new Uint8Array(decodeBinary(entry.data));
            let path = entry.path.substring(baseDir.length);
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (entry.deleted || entry._deleted) {
                this.sendLog(`${path} delete detected`);
                await this.dispatchDeleted(path);
            } else {
                const docData = { ctime: entry.ctime, mtime: entry.mtime, size: entry.size, deleted: entry.deleted || entry._deleted, data: d };
                this.sendLog(`${path} change detected`);
                await this.dispatch(path, docData);
            }
        };
    }

    // Build the interest-check filter (fires for ALL changes, not just matched ones)
    _makeCheckInterested(baseDir: string) {
        return (entry: MetaEntry) => {
            this.lastEventTime = Date.now();
            if (entry.path.indexOf(":") !== -1) return false;
            return entry.path.startsWith(baseDir);
        };
    }

    // Start watching the _changes feed
    _startWatch(baseDir: string) {
        this.man.beginWatch(
            this._makeChangeCallback(baseDir),
            this._makeCheckInterested(baseDir)
        );
    }

    async start(): Promise<void> {
        const baseDir = this.toLocalPath("");
        await this.man.ready.promise;
        const w = await this.man.rawGet<Record<string, any>>(MILESTONE_DOCID);
        if (w && "tweak_values" in w) {
            if (this.config.useRemoteTweaks) {
                const tweaks = Object.values(w["tweak_values"])[0] as TweakValues;
                // console.log(tweaks)
                const orgConf = { ...this.config } as Record<string, any>;
                this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                if (tweaks.encrypt && !this.config.passphrase) {
                    throw new Error("Remote database is encrypted but no passphrase provided.");
                }
                if (tweaks.usePathObfuscation && !this.config.obfuscatePassphrase) {
                    throw new Error("Remote database is obfuscated but no obfuscate passphrase provided.");
                }
                this.config.hashAlg = tweaks.hashAlg ?? this.config.hashAlg;
                this.config.maxAgeInEden = tweaks.maxAgeInEden ?? this.config.maxAgeInEden;
                this.config.maxTotalLengthInEden = tweaks.maxTotalLengthInEden ?? this.config.maxTotalLengthInEden;
                this.config.maxChunksInEden = tweaks.maxChunksInEden ?? this.config.maxChunksInEden;
                this.config.useEden = tweaks.useEden ?? this.config.useEden;
                if (!this.config.enableCompression != !tweaks.enableCompression) {
                    throw new Error("Compression setting mismatched.");
                }
                this.config.useDynamicIterationCount = tweaks.useDynamicIterationCount ?? this.config.useDynamicIterationCount;
                this.config.enableChunkSplitterV2 = tweaks.enableChunkSplitterV2 ?? this.config.enableChunkSplitterV2;
                this.config.chunkSplitterVersion = tweaks.chunkSplitterVersion ?? this.config.chunkSplitterVersion;
                this.config.E2EEAlgorithm = tweaks.E2EEAlgorithm ?? this.config.E2EEAlgorithm;
                this.config.minimumChunkSize = tweaks.minimumChunkSize ?? this.config.minimumChunkSize;
                this.config.customChunkSize = tweaks.customChunkSize ?? this.config.customChunkSize;
                this.config.doNotUseFixedRevisionForChunks = tweaks.doNotUseFixedRevisionForChunks ?? this.config.doNotUseFixedRevisionForChunks;
                this.config.handleFilenameCaseSensitive = tweaks.handleFilenameCaseSensitive ?? this.config.handleFilenameCaseSensitive;
                const newConf = { ...this.config } as Record<string, any>;
                this.man.options = this.config;
                await this.man.liveSyncLocalDB.initializeDatabase()
                // await this.man.managers.initManagers();
                const diff = unique([...Object.keys(orgConf), ...Object.keys(tweaks)]).filter(k => orgConf[k] != newConf[k]);
                if (diff.length > 0) {
                    this.normalLog(`Remote tweaks changed --->`);
                    for (const diffKey of diff) {
                        this.normalLog(`${diffKey}\t: ${orgConf[diffKey]} \t : ${newConf[diffKey]}`);
                    }
                    this.normalLog(`<--- Remote tweaks changed`);
                }
            }
        }
        if (!w) {
            this.normalLog(`Remote database looks like empty. fetch from the first.`);
            this.setSetting("remote-created", "0");
            return;
        }
        const created = w.created;
        if (this.getSetting("remote-created") !== `${created}`) {
            this.man.since = "";
            this.normalLog(`Remote database looks like rebuilt. fetch from the first again.`);
            this.setSetting("remote-created", `${created}`);
        } else {
            this.normalLog(`Watch starting from ${this.man.since}`);
        }

        this._startWatch(baseDir);

        // Watchdog: detect stale _changes feed and reconnect
        this.watchdogInterval = setInterval(async () => {
            try {
                const silenceMs = Date.now() - this.lastEventTime;
                if (silenceMs < PeerCouchDB.WATCHDOG_STALE_MS) return;

                // Skip if the lib's own reconnect is already in progress
                if (!this.man.watching) {
                    this.normalLog(`Watchdog: watch already stopped/reconnecting — skipping`);
                    this.lastEventTime = Date.now();
                    return;
                }

                // Exponential backoff on consecutive reconnects
                const backoffMs = Math.min(
                    PeerCouchDB.WATCHDOG_CHECK_MS * Math.pow(2, this.consecutiveReconnects),
                    PeerCouchDB.MAX_BACKOFF_MS
                );
                if (silenceMs < backoffMs) return;

                // Health check: is CouchDB reachable? Use rawGet to reuse the lib's connection.
                try {
                    const result = await this.man.rawGet<Record<string, any>>(MILESTONE_DOCID);
                    if (!result) throw new Error("unreachable");
                } catch {
                    this.normalLog(`Watchdog: no events for ${Math.round(silenceMs / 1000)}s, CouchDB unreachable — waiting`);
                    return;
                }

                this.consecutiveReconnects++;
                this.normalLog(`Watchdog: no events for ${Math.round(silenceMs / 1000)}s, reconnecting (attempt ${this.consecutiveReconnects})`);

                // Flush pending since value before reconnect
                this._flushSince();

                // Tear down and reconnect. Must clear watching flag directly because
                // endWatch() only sends cancel signal — the complete handler sets
                // watching=false asynchronously which races with beginWatch().
                this.man.endWatch();
                this.man.watching = false;
                this.man.changes = undefined;
                this.man.since = this.getSetting("since") || this.man.since;
                this.normalLog(`Watchdog: reconnecting from since=${this.man.since}`);
                this.lastEventTime = Date.now();
                this._startWatch(baseDir);
            } catch (err) {
                this.normalLog(`Watchdog: unexpected error: ${err}`);
                this.lastEventTime = Date.now(); // prevent tight retry loop
            }
        }, PeerCouchDB.WATCHDOG_CHECK_MS);
    }
    async dispatch(path: string, data: FileData | false) {
        if (data === false) return;
        if (!await this.isRepeating(path, data)) {
            await this.dispatchToHub(this, this.toGlobalPath(path), data);
        }
        // else {
        //     this.receiveLog(`${path} dispatch repeating`);
        // }
    }
    async dispatchDeleted(path: string) {
        if (!await this.isRepeating(path, false)) {
            await this.dispatchToHub(this, this.toGlobalPath(path), false);
        }
    }
    async stop(): Promise<void> {
        if (this.watchdogInterval) {
            clearInterval(this.watchdogInterval);
            this.watchdogInterval = undefined;
        }
        this._flushSince();
        this.man.endWatch();
        return await Promise.resolve();
    }
}
