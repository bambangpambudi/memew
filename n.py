

'use strict';

const crypto = require('node:crypto');
const readline = require('node:readline');

// ---------- Styling (ANSI, mirrors colorama/termcolor output) ----------
const ESC = '\x1b[';
const RESET = ESC + '0m';
const BRIGHT = ESC + '1m';
const DIM = ESC + '2m';

const CYAN = ESC + '36m';
const GREEN = ESC + '32m';
const YELLOW = ESC + '33m';
const RED = ESC + '31m';
const MAGENTA = ESC + '35m';
const BLUE = ESC + '34m';
const WHITE = ESC + '37m';

const LIGHT_CYAN = ESC + '96m';
const LIGHT_GREEN = ESC + '92m';
const LIGHT_BLUE = ESC + '94m';
const LIGHT_MAGENTA = ESC + '95m';
const LIGHT_YELLOW = ESC + '93m';
const LIGHT_RED = ESC + '91m';

// ---------- Target ----------
const TARGET_SPEAKER = 'C';
const TARGET_NAME = 'NoTrack';

// ---------- Futuristic Banner ----------
const BANNER = `
${LIGHT_BLUE}${BRIGHT}╭─────────────────────────────────────────────╮${RESET}
${LIGHT_BLUE}${BRIGHT}│                                             │${RESET}
${LIGHT_CYAN}${BRIGHT}│  ██████╗   ████████╗███████╗ ██████╗██╗  ██╗ │${RESET}
${LIGHT_CYAN}${BRIGHT}│  ██╔══██╗  ╚══██╔══╝██╔════╝██╔════╝██║  ██║ │${RESET}
${LIGHT_CYAN}${BRIGHT}│  ██████╔╝     ██║   █████╗  ██║     ███████║ │${RESET}
${LIGHT_CYAN}${BRIGHT}│  ██╔══██╗     ██║   ██╔══╝  ██║     ██╔══██║ │${RESET}
${LIGHT_CYAN}${BRIGHT}│  ██████╔╝██   ██║   ███████╗╚██████╗██║  ██║ │${RESET}
${LIGHT_CYAN}${BRIGHT}│  ╚═════╝ ╚═   ╚═╝   ╚══════╝ ╚═════╝╚═╝  ╚═╝ │${RESET}
${LIGHT_BLUE}${BRIGHT}│                                             │${RESET}
${LIGHT_BLUE}${BRIGHT}│          𝗕𝗘𝗡𝗙𝗘𝗜 𝗧𝗘𝗖𝗛                 │${RESET}
${LIGHT_BLUE}${BRIGHT}│        𝗨𝗡𝗖𝗘𝗡𝗦𝗢𝗥𝗘𝗗 𝗔𝗜               │${RESET}
${LIGHT_BLUE}${BRIGHT}│${DIM}${WHITE}          ◆  AI STREAM INTERFACE  ◆          ${RESET}${LIGHT_BLUE}${BRIGHT}│${RESET}
${LIGHT_BLUE}${BRIGHT}│                                             │${RESET}
${LIGHT_BLUE}${BRIGHT}╰─────────────────────────────────────────────╯${RESET}
`;

// ---------- Config ----------
const BASE_URL = 'https://notrack.ai';
const HEARTBEAT_URL = 'https://statinside.com/api/send-heartbeat';
const HEARTBEAT_INTERVAL = 3000; // ms

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
  'AppleWebKit/537.36 (KHTML, like Gecko) ' +
  'Chrome/120.0.0.0 Safari/537.36';

const HEADERS = {
  'User-Agent': USER_AGENT,
  'Accept': 'text/event-stream',
  'Accept-Language': 'en-US,en;q=0.9',
  'Content-Type': 'application/json',
  'Origin': BASE_URL,
  'Referer': `${BASE_URL}/chat`,
  'Cache-Control': 'no-cache',
};

// ---------- Cookie Jar ----------
const cookieJar = new Map();
function setCookie(name, value) {
  cookieJar.set(name, value);
}
function getCookieHeader() {
  return [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
}

// Anonymous browser-style identifiers.
// Keep them alive for the whole terminal session.
const SI_USER_ID = crypto.randomUUID().replace(/-/g, '');
const SI_SESSION_ID = crypto.randomUUID().replace(/-/g, '');
const UID = crypto.randomUUID();

setCookie('si_usr_id', SI_USER_ID);
setCookie('si_ses_id', SI_SESSION_ID);
setCookie('uid', UID);

// ---------- Heartbeat ----------
function startHeartbeat() {
  const timer = setInterval(async () => {
    try {
      await fetch(HEARTBEAT_URL, {
        method: 'POST',
        headers: {
          'User-Agent': USER_AGENT,
          'Content-Type': 'application/json',
          'Cookie': getCookieHeader(),
        },
        body: JSON.stringify({
          id: 43,
          event: 'heartbeat',
          ts: Date.now(),
        }),
      });
    } catch (_) {
      // Runs silently in background.
    }
  }, HEARTBEAT_INTERVAL);

  // Do not keep the process alive solely because of the heartbeat.
  if (typeof timer.unref === 'function') timer.unref();
}

// ---------- SSE Decoder ----------
async function* iterSSEEvents(response) {
  /**
   * Properly assemble Server-Sent Events.
   *
   * Handles:
   *  - data: ...
   *  - multiple data lines
   *  - blank-line event separators
   *  - data: without a space
   */
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let dataLines = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    let idx;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      let line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);

      if (line.endsWith('\r')) line = line.slice(0, -1);

      // Blank line = end of SSE event
      if (line === '') {
        if (dataLines.length > 0) {
          yield dataLines.join('\n');
          dataLines = [];
        }
        continue;
      }

      if (line.startsWith('data:')) {
        let v = line.slice(5);
        if (v.startsWith(' ')) v = v.slice(1);
        dataLines.push(v);
      }
    }
  }

  // Flush final event
  if (dataLines.length > 0) {
    yield dataLines.join('\n');
  }
}

// ---------- Dispatch ----------
async function sendPrompt(chatId, promptText, quiet = true) {
  /**
   * Send prompt and stream only the target AI response.
   *
   * Returns:
   *   [newChatId, fullResponseText]
   */
  const url = `${BASE_URL}/api/dispatch`;

  const payload = {
    user_input: promptText,
    mode: 'usual',
    model: 'C',
    persona: 'normal',
    max_turns: 6,
    chat_id: chatId || null,
    attachments: [],
    regenerate: false,
  };

  let newChatId = null;
  let notrackText = '';
  let printing = false;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 180 * 1000);

  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        ...HEADERS,
        'Cookie': getCookieHeader(),
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    if (!resp.ok) {
      const err = new Error(`HTTP ${resp.status} ${resp.statusText}`);
      err.name = 'HTTPError';
      throw err;
    }

    if (!quiet) {
      process.stdout.write(
        `${DIM}${LIGHT_BLUE}[ SYSTEM ] HTTP ${resp.status}${RESET}\n`
      );
    }

    for await (const dataStr of iterSSEEvents(resp)) {
      const trimmed = dataStr.trim();
      if (!trimmed) continue;

      let evt;
      try {
        evt = JSON.parse(trimmed);
      } catch (_) {
        if (!quiet) {
          process.stdout.write(
            `${DIM}${YELLOW}[ STREAM ] Non-JSON event: ` +
            `${trimmed.slice(0, 200)}${RESET}\n`
          );
        }
        continue;
      }

      const etype = evt.type;
      const speaker = evt.speaker;

      // --------------------------------------------
      // Session bookkeeping
      // --------------------------------------------
      if (etype === 'chat_meta') {
        const cid = evt.chat_id;
        if (cid && !newChatId) newChatId = cid;

        if (!quiet) {
          process.stdout.write(
            `${DIM}${LIGHT_BLUE}[ SESSION ] ` +
            `${WHITE}chat_id=${cid} ` +
            `mode=${evt.mode} model=${evt.model}` +
            `${RESET}\n`
          );
        }
        continue;
      }

      // --------------------------------------------
      // Only target speaker
      // --------------------------------------------
      if (speaker !== TARGET_SPEAKER) continue;

      // --------------------------------------------
      // AI thinking
      // --------------------------------------------
      if (etype === 'thinking') {
        process.stdout.write(
          `\n${LIGHT_MAGENTA}${BRIGHT}` +
          `╭─[ ⚡ 𝗨𝗡𝗖𝗘𝗡𝗦𝗢𝗥𝗘𝗗 𝗔𝗜 ]` +
          `──────────────╮` +
          `${RESET}\n`
        );
        process.stdout.write(`${LIGHT_CYAN}${BRIGHT}│ ${RESET}`);
        printing = true;
      }

      // --------------------------------------------
      // Streaming delta
      // --------------------------------------------
      else if (etype === 'delta') {
        let chunk = evt.chunk || '';
        if (typeof chunk !== 'string') continue;

        // Remove internal thinking markers.
        chunk = chunk.replace(/<\/?think>/g, '');

        if (!chunk) continue;

        process.stdout.write(`${LIGHT_CYAN}${chunk}${RESET}`);
        notrackText += chunk;
      }

      // --------------------------------------------
      // Final consolidated message
      // --------------------------------------------
      else if (etype === 'message') {
        let content = evt.content;
        if (typeof content !== 'string') content = '';

        if (notrackText.trim() === '' && content.trim()) {
          notrackText = content;
          process.stdout.write(`${LIGHT_CYAN}${content}${RESET}`);
        }

        if (printing) {
          process.stdout.write(
            `\n${LIGHT_MAGENTA}${BRIGHT}` +
            `╰──────────────────────────────────────╯` +
            `${RESET}\n`
          );
          printing = false;
        }
      }

      // --------------------------------------------
      // Error
      // --------------------------------------------
      else if (etype === 'error') {
        process.stdout.write(
          `\n${LIGHT_RED}${BRIGHT}` +
          `╭─[ ✖ 𝗘𝗥𝗥𝗢𝗥 ]────────────────────────╮` +
          `${RESET}\n`
        );
        process.stdout.write(`${RED}${evt.content}${RESET}\n`);
        process.stdout.write(
          `${LIGHT_RED}${BRIGHT}` +
          `╰──────────────────────────────────────╯` +
          `${RESET}\n`
        );
      }

      // --------------------------------------------
      // Done
      // --------------------------------------------
      else if (etype === 'done') {
        if (printing) {
          process.stdout.write(
            `\n${LIGHT_MAGENTA}${BRIGHT}` +
            `╰──────────────────────────────────────╯` +
            `${RESET}\n`
          );
          printing = false;
        }
      }
    }

    process.stdout.write('\n');
    return [newChatId, notrackText];
  } catch (e) {
    if (e.name === 'AbortError') {
      process.stdout.write(
        `\n${LIGHT_RED}${BRIGHT}` +
        `╭─[ ✖ 𝗧𝗜𝗠𝗘𝗢𝗨𝗧 ]───────────────────────╮` +
        `${RESET}\n`
      );
      process.stdout.write(
        `${RED}The AI took too long to return the stream.${RESET}\n`
      );
      process.stdout.write(
        `${LIGHT_RED}${BRIGHT}` +
        `╰──────────────────────────────────────╯` +
        `${RESET}\n`
      );
    } else if (e.name === 'HTTPError') {
      process.stdout.write(
        `\n${LIGHT_RED}${BRIGHT}` +
        `╭─[ ✖ 𝗛𝗧𝗧𝗣 𝗘𝗥𝗥𝗢𝗥 ]──────────────────╮` +
        `${RESET}\n`
      );
      process.stdout.write(`${RED}${e.message}${RESET}\n`);
      process.stdout.write(
        `${LIGHT_RED}${BRIGHT}` +
        `╰──────────────────────────────────────╯` +
        `${RESET}\n`
      );
    } else {
      process.stdout.write(
        `\n${LIGHT_RED}${BRIGHT}` +
        `╭─[ ✖ 𝗦𝗧𝗥𝗘𝗔𝗠 𝗘𝗥𝗥𝗢𝗥 ]─────────────────╮` +
        `${RESET}\n`
      );
      process.stdout.write(`${RED}${e.message || e}${RESET}\n`);
      process.stdout.write(
        `${LIGHT_RED}${BRIGHT}` +
        `╰──────────────────────────────────────╯` +
        `${RESET}\n`
      );
    }
    return [newChatId, notrackText];
  } finally {
    clearTimeout(timeoutId);
  }
}

// ---------- Main ----------
async function main() {
  process.stdout.write(BANNER + '\n');

  process.stdout.write(
    `${LIGHT_BLUE}${BRIGHT}` +
    `┌─[ 𝗦𝗬𝗦𝗧𝗘𝗠 ]────────────────────────────┐` +
    `${RESET}\n`
  );
  process.stdout.write(
    `${LIGHT_BLUE}│ ${LIGHT_CYAN}` +
    `Initializing ${BRIGHT}𝗨𝗡𝗖𝗘𝗡𝗦𝗢𝗥𝗘𝗗 𝗔𝗜${RESET}\n`
  );
  process.stdout.write(
    `${LIGHT_BLUE}│ ${DIM}${WHITE}` +
    `AI stream interface starting...` +
    `${RESET}\n`
  );
  process.stdout.write(
    `${LIGHT_BLUE}${BRIGHT}` +
    `└───────────────────────────────────────────┘` +
    `${RESET}\n`
  );

  // Start background heartbeat silently.
  startHeartbeat();

  let chatId = null;

  process.stdout.write(
    `\n${LIGHT_GREEN}${BRIGHT}` +
    `╭─[ ✓ 𝗥𝗘𝗔𝗗𝗬 ]────────────────────────────╮` +
    `${RESET}\n`
  );
  process.stdout.write(
    `${LIGHT_GREEN}│ ${WHITE}` +
    `𝗨𝗡𝗖𝗘𝗡𝗦𝗢𝗥𝗘𝗗 𝗔𝗜 interface is online.\n`
  );
  process.stdout.write(
    `${LIGHT_GREEN}│ ${DIM}${WHITE}` +
    `'exit' quit · 'new' reset` +
    `${RESET}\n`
  );
  process.stdout.write(
    `${LIGHT_GREEN}${BRIGHT}` +
    `╰───────────────────────────────────────────╯` +
    `${RESET}\n\n`
  );

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const question = (q) =>
    new Promise((resolve) => rl.question(q, resolve));

  // Ctrl+C handling
  let interrupted = false;
  process.on('SIGINT', () => {
    if (interrupted) process.exit(0);
    interrupted = true;

    process.stdout.write(
      `\n${LIGHT_YELLOW}${BRIGHT}\n` +
      `╭─[ ⚠ 𝗜𝗡𝗧𝗘𝗥𝗥𝗨𝗣𝗧𝗘𝗗 ]───────────────────╮` +
      `${RESET}\n`
    );
    process.stdout.write(
      `${LIGHT_YELLOW}│ ${WHITE}` +
      `Exiting 𝗕𝗘𝗡𝗙𝗘𝗜 𝗧𝗘𝗖𝗛 client...\n`
    );
    process.stdout.write(
      `${LIGHT_YELLOW}${BRIGHT}` +
      `╰───────────────────────────────────────────╯` +
      `${RESET}\n`
    );

    try { rl.close(); } catch (_) {}
    process.exit(0);
  });

  while (true) {
    let userInput;
    try {
      userInput = (
        await question(
          `${LIGHT_MAGENTA}${BRIGHT}` +
          `╭─[ 𝗕𝗘𝗡𝗙𝗘𝗜 𝗧𝗘𝗖𝗛 ] ` +
          `${LIGHT_CYAN}› ${RESET}`
        )
      ).trim();
    } catch (_) {
      // readline was closed
      break;
    }

    if (!userInput) continue;

    // --------------------------------------------
    // Exit
    // --------------------------------------------
    if (['exit', 'quit', 'q'].includes(userInput.toLowerCase())) {
      process.stdout.write(
        `\n${LIGHT_YELLOW}${BRIGHT}` +
        `╭─[ 𝗦𝗬𝗦𝗧𝗘𝗠 ]───────────────────────────╮` +
        `${RESET}\n`
      );
      process.stdout.write(
        `${LIGHT_YELLOW}│ ${WHITE}Session terminated.\n`
      );
      process.stdout.write(
        `${LIGHT_YELLOW}│ ` +
        `${LIGHT_CYAN}${BRIGHT}𝗕𝗘𝗡𝗙𝗘𝗜 𝗧𝗘𝗖𝗛` +
        `${WHITE} signing off.\n`
      );
      process.stdout.write(
        `${LIGHT_YELLOW}${BRIGHT}` +
        `╰───────────────────────────────────────────╯` +
        `${RESET}\n`
      );
      rl.close();
      break;
    }

    // --------------------------------------------
    // New session
    // --------------------------------------------
    if (userInput.toLowerCase() === 'new') {
      chatId = null;
      process.stdout.write(
        `${LIGHT_YELLOW}${BRIGHT}` +
        `╭─[ ↻ 𝗡𝗘𝗪 𝗦𝗘𝗦𝗦𝗜𝗢𝗡 ]──────────────────╮` +
        `${RESET}\n`
      );
      process.stdout.write(
        `${LIGHT_YELLOW}│ ${WHITE}` +
        `Fresh AI conversation initialized.\n`
      );
      process.stdout.write(
        `${LIGHT_YELLOW}${BRIGHT}` +
        `╰───────────────────────────────────────────╯` +
        `${RESET}\n\n`
      );
      continue;
    }

    // --------------------------------------------
    // Send prompt
    // --------------------------------------------
    try {
      const [newId, _reply] = await sendPrompt(chatId, userInput);
      if (newId) chatId = newId;
    } catch (e) {
      process.stdout.write(
        `${LIGHT_RED}${BRIGHT}` +
        `\n╭─[ ✖ 𝗨𝗡𝗘𝗫𝗣𝗘𝗖𝗧𝗘𝗗 𝗘𝗥𝗥𝗢𝗥 ]────────────╮` +
        `${RESET}\n`
      );
      process.stdout.write(`${RED}│ ${e.message || e}${RESET}\n`);
      process.stdout.write(
        `${LIGHT_RED}${BRIGHT}` +
        `╰───────────────────────────────────────────╯` +
        `${RESET}\n`
      );
    }
  }
}

// ---------- Entry ----------
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
