const CACHE_NAME = "spedscan-v1";
const APP_SHELL = [
  "/",
  "/static/index.html",
  "/static/today.html",
  "/static/setup.html",
  "/static/sw.js"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

// Cache-first for app shell, network-first for API
self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // API calls: try network first
  if (url.pathname.startsWith("/students") || url.pathname.startsWith("/scan") || url.pathname.startsWith("/pending") || url.pathname.startsWith("/logs") || url.pathname.startsWith("/api/") || url.pathname === "/health") {
    event.respondWith(
      fetch(event.request).catch(() => caches.match(event.request))
    );
    return;
  }

  // Static/app routes: cache first
  event.respondWith(
    caches.match(event.request).then((cached) => cached || fetch(event.request))
  );
});
