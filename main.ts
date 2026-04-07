import { defaultLoggerEnv } from "./lib/src/common/logger.ts";
import { LOG_LEVEL_DEBUG } from "./lib/src/common/logger.ts";
import { Hub } from "./Hub.ts";
import { Config } from "./types.ts";
import { parseArgs } from "jsr:@std/cli";

// Prevent CouchDB connection errors from crashing the process.
// The _changes feed and chunk uploads use node-fetch which throws
// unhandled FetchError/TypeError on connection drops. The watchdog
// in PeerCouchDB handles reconnection — we just need to survive.
globalThis.addEventListener("unhandledrejection", (event) => {
    const reason = event.reason;
    const msg = reason?.message ?? String(reason);
    // Suppress known transient CouchDB connection errors
    if (msg.includes("connection error") ||
        msg.includes("error reading a body from connection") ||
        msg.includes("getDBEntryMeta")) {
        event.preventDefault();
        console.warn(`[suppressed] ${msg}`);
        return;
    }
    // Let other unhandled rejections crash as before
});

const KEY = "LSB_"
defaultLoggerEnv.minLogLevel = LOG_LEVEL_DEBUG;
const configFile = Deno.env.get(`${KEY}CONFIG`) || "./dat/config.json";

console.log("LiveSync Bridge is now starting...");
let config: Config = { peers: [] };
const flags = parseArgs(Deno.args, {
    boolean: ["reset"],
    // string: ["version"],
    default: { reset: false },
});
if (flags.reset) {
    localStorage.clear();
}
try {
    const confText = await Deno.readTextFile(configFile);
    config = JSON.parse(confText);
} catch (ex) {
    console.error("Could not parse configuration!");
    console.error(ex);
}
console.log("LiveSync Bridge is now started!");
const hub = new Hub(config);
hub.start();