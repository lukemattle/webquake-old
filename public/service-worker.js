const CACHE_NAME = 'webquake-cache-v1';
const urlsToCache = [
  '/',
  '/index.html',
  '/OneSignalSDKWorker.js',
  '/icons/webquake-192.png',
  '/icons/webquake-512.png'
];

self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(function(cache) {
        return cache.addAll(urlsToCache);
      })
  );
});

self.addEventListener('fetch', function(event) {
  event.respondWith(
    caches.match(event.request)
      .then(function(response) {
        return response || fetch(event.request);
      })
  );
});