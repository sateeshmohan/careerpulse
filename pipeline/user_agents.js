const DESKTOP_USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:121.0) Gecko/20100101 Firefox/121.0"
];

const MOBILE_USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36"
];

const USER_AGENTS = DESKTOP_USER_AGENTS.concat(MOBILE_USER_AGENTS);

function randomFrom(list) {
  if (!Array.isArray(list) || !list.length) {
    return "";
  }
  return list[Math.floor(Math.random() * list.length)];
}

function randomUserAgent(options = {}) {
  const allowMobile = options.allowMobile !== false;
  const pool = allowMobile ? USER_AGENTS : DESKTOP_USER_AGENTS;
  return randomFrom(pool);
}

function randomDesktopUserAgent() {
  return randomFrom(DESKTOP_USER_AGENTS);
}

module.exports = {
  DESKTOP_USER_AGENTS,
  MOBILE_USER_AGENTS,
  USER_AGENTS,
  randomUserAgent,
  randomDesktopUserAgent
};
