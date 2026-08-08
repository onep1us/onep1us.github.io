/* Service Worker — 绕过浏览器 10 分钟页面缓存，让博客每次打开都是最新内容。
   页面导航：始终从网络拉取；离线时回退到缓存。
   静态资源：缓存优先即时返回，后台异步更新。 */
var CACHE_NAME = 'onep1us-blog-v1';

self.addEventListener('install', function (event) {
  self.skipWaiting();
});

self.addEventListener('activate', function (event) {
  event.waitUntil(
    caches.keys().then(function (keys) {
      return Promise.all(
        keys.filter(function (key) { return key !== CACHE_NAME; })
            .map(function (key) { return caches.delete(key); })
      );
    }).then(function () {
      return self.clients.claim();
    })
  );
});

self.addEventListener('fetch', function (event) {
  var request = event.request;
  if (request.method !== 'GET') { return; }

  /* 页面导航：绕过浏览器 HTTP 缓存，强制从网络拿最新页面 */
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request, { cache: 'no-store' }).then(function (response) {
        if (response && response.ok) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function (cache) { cache.put(request, clone); });
        }
        return response;
      }).catch(function () {
        return caches.match(request);
      })
    );
    return;
  }

  /* 静态资源：缓存优先，后台异步更新（stale-while-revalidate） */
  event.respondWith(
    caches.match(request).then(function (cached) {
      var network = fetch(request, { cache: 'no-store' }).then(function (response) {
        if (response && response.ok) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function (cache) { cache.put(request, clone); });
        }
        return response;
      });
      return cached || network;
    })
  );
});
