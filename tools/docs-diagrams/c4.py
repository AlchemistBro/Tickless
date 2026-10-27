#!/usr/bin/env python3
"""Схемы C4 для README: контекст, контейнеры, развёртывание. Запуск: python tools/docs-diagrams/c4.py."""

from svgkit import ROOT, Canvas

OUT = "docs/c4"

LEGEND = [
    ("person", "Пользователь Tickless [Person]"),
    ("person-ext", "Внешний клиент [Person]"),
    ("container", "Контейнер Tickless [Container]"),
    ("external", "Внешняя система [Software System]"),
    ("both", "Запрос и ответ: 1-я строка — запрос, «Ответ:» — что возвращается"),
    ("line", "Передача в одну сторону"),
]


def hl(c, a, b, y):
    """Прямая горизонтальная связь на высоте y: a левее b."""
    xa, ya, wa, ha = c.shapes[a]
    xb, yb, wb, hb = c.shapes[b]
    return (a, "r", round((y - ya) / ha, 4)), (b, "l", round((y - yb) / hb, 4))


def vl(c, a, b, x):
    """Прямая вертикальная связь на абсциссе x: a выше b."""
    xa, ya, wa, ha = c.shapes[a]
    xb, yb, wb, hb = c.shapes[b]
    return (a, "b", round((x - xa) / wa, 4)), (b, "t", round((x - xb) / wb, 4))


def straight_h(c, a, b, y, label, ax=None, **kw):
    src, dst = hl(c, a, b, y)
    xa, _, wa, _ = c.shapes[a]
    xb = c.shapes[b][0]
    c.edge(src, dst, label=label, at=(ax if ax else (xa + wa + xb) / 2, y), **kw)


def straight_v(c, a, b, x, label, at_y=None, **kw):
    src, dst = vl(c, a, b, x)
    _, ya, _, ha = c.shapes[a]
    yb = c.shapes[b][1]
    c.edge(src, dst, label=label, at=(x, at_y if at_y else (ya + ha + yb) / 2), **kw)


def context():
    c = Canvas("C4 · контекст", 2000, 1300)
    c.person("trader", 40, 40, 290, 200, "Трейдер", "Person", "Регистрируется, пополняет счёт, торгует, выводит средства")
    c.person("bot", 40, 330, 290, 200, "Бот / API-клиент", "Person", "Внешняя программа с HMAC-ключом API", kind="person-ext")
    c.person("admin", 40, 620, 290, 200, "Администратор", "Person", "Замораживает пользователей, одобряет крупные выводы")
    c.box("tickless", 790, 40, 400, 780, "Tickless", "Software System",
          "Спотовая биржа: лимитные и рыночные ордера, двойная бухгалтерия, рыночные данные и свечи, депозиты и выводы BTC, ETH и USDT",
          kind="container", size=26)
    c.box("btc", 1640, 60, 320, 150, "Bitcoin Core", "Software System · regtest", "Блоки, транзакции и баланс для BTC", kind="external", size=20)
    c.box("eth", 1640, 270, 320, 150, "Anvil", "Software System · devnet", "ETH и тестовый ERC-20 USDT", kind="external", size=20)
    c.box("mail", 1640, 480, 320, 150, "Mailpit", "Software System", "Принимает письма пользователям", kind="external", size=20)
    c.box("obs", 1640, 690, 320, 150, "Наблюдаемость", "Software System", "Prometheus, Grafana, Jaeger, Alertmanager", kind="external", size=20)

    def person_y(ident):
        x, top, w, h = c.shapes[ident]
        return top + h / 2

    straight_h(c, "trader", "tickless", person_y("trader"), ("Торгует, выводит средства", "Ответ: балансы, исполнения"), both=True)
    straight_h(c, "bot", "tickless", person_y("bot"), ("Ордера, рыночные данные", "Ответ: статусы, события", "[REST, WebSocket · HMAC]"), both=True)
    straight_h(c, "admin", "tickless", person_y("admin"), ("Одобряет выводы, замораживает", "Ответ: очередь ревью"), both=True)
    straight_h(c, "tickless", "btc", 135, ("Депозиты, выводы BTC", "Ответ: блоки, статус", "[JSON-RPC]"), both=True)
    straight_h(c, "tickless", "eth", 345, ("Депозиты, выводы ETH и USDT", "Ответ: блоки, статус", "[JSON-RPC]"), both=True)
    straight_h(c, "tickless", "mail", 555, ("Письма пользователям", "[SMTP]"))
    straight_h(c, "tickless", "obs", 765, ("Метрики, трассы, логи", "[OTLP]"), dashed=True)
    c.note(790, 850, 400, "Приватные ключи горячего кошелька есть только у сервиса signer внутри системы. Сети только regtest и devnet.")
    c.legend(40, 960, LEGEND, width=560)
    return c.write(f"{OUT}/c4-context.drawio.svg")


def containers():
    c = Canvas("C4 · контейнеры", 2710, 1440)
    c.person("users", 20, 30, 240, 230, "Трейдер и администратор", "Person", "Работают в браузере")
    c.person("bot", 20, 414, 240, 230, "Бот / API-клиент", "Person", "Внешняя программа", kind="person-ext")

    c.boundary(480, 40, 400, 760, "Сегмент доступа", "Сетевой сегмент · edge")
    c.box("web", 520, 110, 320, 130, "Веб-терминал web", "Container: React, nginx", "Статика SPA; /v1 и /ws проксируются в gateway", size=17)
    c.box("gateway", 520, 330, 320, 360, "gateway", "Container: Go, chi", "Единственный публичный вход: REST и WebSocket, JWT и HMAC, лимиты, идемпотентность, проверка заморозки", size=19)

    c.boundary(1030, 60, 1000, 470, "Торговое ядро", "Container: Go · gRPC · своя база у каждого", ident="core")
    grid = [
        ("identity", "identity", "Пользователи, сессии, TOTP, ключи API"),
        ("orders", "orders", "Сага ордера, правила рынков, идемпотентность"),
        ("ledger", "ledger", "Двойная запись, холды, расчёт сделок"),
        ("matching", "matching", "Книги в памяти, детерминированный движок"),
        ("marketdata", "marketdata", "Книга, сделки, тикер, публичный WebSocket"),
        ("candles", "candles", "Свечи OHLCV из потока сделок"),
    ]
    for i, (ident, name, desc) in enumerate(grid):
        c.box(ident, 1060 + 320 * (i % 3), 110 + 190 * (i // 3), 300, 150, name, "Container: Go", desc, size=18)

    c.boundary(1030, 600, 1000, 470, "Кошельки, выводы и письма", "Container: Go · gRPC", ident="wallets")
    grid = [
        ("wallet", "wallet", "Адреса депозита из xpub, без приватных ключей"),
        ("chainwatcher", "chainwatcher", "Сканеры блоков, подтверждения, откат при реорге"),
        ("withdrawals", "withdrawals", "Автомат состояний, лимиты, проверка, ревью"),
        ("signer", "signer", "Единственный держатель ключа горячего кошелька, только mTLS"),
        ("notifier", "notifier", "Письма, повторы с паузами, очередь недоставленных"),
    ]
    for i, (ident, name, desc) in enumerate(grid):
        c.box(ident, 1060 + 320 * (i % 3), 650 + 190 * (i // 3), 300, 150, name, "Container: Go", desc, size=18)

    c.boundary(2290, 60, 380, 1010, "Сегмент данных", "Сетевой сегмент · data", ident="data")
    c.database("pg", 2310, 110, 340, 150, "PostgreSQL + PgBouncer", "Container", "Своя база на сервис, outbox", size=17)
    c.queue("kafka", 2310, 300, 340, 150, "Kafka", "Container: KRaft, 1 брокер", "События и команды, ключ — рынок", size=17)
    c.database("redis", 2310, 490, 340, 150, "Redis", "Container", "Лимиты, идемпотентность, снимки рынка", size=17)
    c.database("ch", 2310, 680, 340, 150, "ClickHouse", "Container", "Сделки и материализованные свечи", size=17)
    c.queue("rabbit", 2310, 870, 340, 150, "RabbitMQ", "Container", "Задачи писем и ревью, повторы, DLQ", size=17)

    c.box("btc", 1080, 1190, 280, 150, "bitcoind", "Software System · regtest", "Блоки и транзакции BTC", kind="external", size=18)
    c.box("eth", 1420, 1190, 280, 150, "Anvil", "Software System · devnet", "ETH и тестовый USDT", kind="external", size=18)
    c.box("mail", 1760, 1190, 270, 150, "Mailpit", "Software System", "Принимает письма", kind="external", size=18)
    c.box("obs", 2290, 1190, 380, 150, "Наблюдаемость", "Software System", "OTel Collector, Prometheus, Grafana, Jaeger, Alertmanager", kind="external", size=18)

    def person_y(ident):
        x, top, w, h = c.shapes[ident]
        return top + h / 2

    straight_h(c, "users", "web", 175, ("Открывают терминал", "Ответ: страницы"), ax=370, both=True)
    straight_h(c, "bot", "gateway", person_y("bot"), ("Ордера, рыночные данные", "Ответ: статусы, события", "[REST, WebSocket · HMAC]"), ax=370, both=True)
    straight_v(c, "web", "gateway", 680, ("Запросы", "[HTTP]"), both=True)
    straight_h(c, "gateway", "core", 410, ("Команды", "Ответ: результат", "[gRPC]"), ax=955, both=True)
    straight_h(c, "gateway", "wallets", 680, ("Выводы, адреса", "Ответ: статус, адрес", "[gRPC]"), ax=955, both=True)
    straight_h(c, "core", "data", 300, ("SQL, события,", "лимиты, свечи", "[Postgres, Kafka, Redis, CH]"), ax=2140)
    straight_h(c, "wallets", "data", 830, ("SQL, события,", "задачи", "[Postgres, Kafka, AMQP]"), ax=2140)
    straight_v(c, "wallets", "btc", 1220, ("Блоки, broadcast", "[JSON-RPC]"), both=True)
    straight_v(c, "wallets", "eth", 1560, ("Блоки, broadcast", "[JSON-RPC]"), both=True)
    straight_v(c, "wallets", "mail", 1895, ("Письма", "[SMTP]"))

    c.legend(20, 860, [
        ("person", "Пользователь Tickless [Person]"),
        ("person-ext", "Внешний клиент [Person]"),
        ("container", "Контейнер Tickless [Container]"),
        ("external", "Внешняя система [Software System]"),
        ("both", "Запрос и ответ: 1-я строка — запрос, «Ответ:» — что возвращается"),
        ("line", "Передача в одну сторону"),
    ], width=560)
    c.note(20, 1120, 800, "Все события публикуются через outbox в той же транзакции, что и изменение данных, потребители идемпотентны. "
                         "Метрики, трассы и логи всех сервисов уходят в OTel Collector.")
    return c.write(f"{OUT}/c4-containers.drawio.svg")


def deployment():
    c = Canvas("C4 · развёртывание", 2300, 1850)
    c.person("user", 20, 100, 200, 210, "Пользователь", "Person", "Браузер на своём устройстве")
    c.node(320, 20, 1960, 1530, "Хост: Docker Desktop или Linux", "Deployment Node: Docker Compose, сеть tickless, make up", fill="#FFFFFF")

    c.node(350, 90, 360, 230, "Вход", "Deployment Node: compose", fill="#F4F6F8")
    c.box("web", 380, 150, 300, 140, "web (8088)", "Container: nginx", "Статика SPA", size=17)

    c.node(350, 380, 1180, 660, "Приложение", "Deployment Node: compose", fill="#F4F6F8", ident="app")
    c.box("gateway", 380, 470, 240, 150, "gateway (18080)", "Container: Go", "REST, WebSocket", size=17)
    c.boundary(770, 420, 730, 570, "Сервисы приложения", "Container: Go · gRPC", ident="svc")
    names = [("identity (9103)", ""), ("orders (9102)", ""), ("ledger (9101)", ""),
             ("matching", "том matchingdata"), ("marketdata (8081)", ""), ("candles", ""),
             ("wallet", ""), ("chainwatcher", ""), ("withdrawals", "в compose: фаза 6"),
             ("signer", "в compose: фаза 6"), ("notifier", "в compose: фаза 6")]
    for i, (name, desc) in enumerate(names):
        kind = "container"
        c.box(f"s{i}", 790 + 240 * (i % 3), 450 + 120 * (i // 3), 210, 100, name, "Container", desc, kind=kind, size=16)
    c.note(380, 660, 360, "Сервисы withdrawals, signer и notifier написаны и проверены тестами; в compose они добавляются в фазе 6.")

    c.node(1700, 380, 540, 660, "Данные и очереди", "Deployment Node: compose", fill="#F4F6F8", ident="data")
    c.database("pg", 1730, 450, 240, 150, "postgres (15433)", "Container", "+ pgbouncer (6432)", size=16)
    c.queue("kafka", 1990, 450, 220, 150, "kafka (19092)", "Container: KRaft", "один брокер", size=16)
    c.database("redis", 1730, 640, 240, 150, "redis (6379)", "Container", "", size=16)
    c.database("ch", 1990, 640, 220, 150, "clickhouse (8123)", "Container", "", size=15)
    c.queue("rabbit", 1730, 830, 240, 150, "rabbitmq (5672)", "Container", "консоль 15672", size=16)
    c.note(1990, 850, 230, "Тома: pgdata, kafkadata, chdata, rabbitdata.")

    c.node(350, 1130, 700, 330, "Блокчейн-ноды", "Deployment Node: compose", fill="#F4F6F8")
    c.box("btc", 380, 1200, 320, 150, "bitcoind (18443)", "Container: regtest", "том btcdata", kind="external", size=17)
    c.box("eth", 710, 1200, 310, 150, "anvil (8545)", "Container: devnet", "том anvildata", kind="external", size=17)
    c.note(380, 1370, 640, "btc-init, eth-init и kafka-init: одноразовые задачи подготовки.")

    c.node(1100, 1130, 1140, 330, "Наблюдаемость и почта", "Deployment Node: compose", fill="#F4F6F8")
    obs = [("otel", "otel-collector", "OTLP 14317"), ("prom", "prometheus (9090)", "+ alertmanager"), ("graf", "grafana (13000)", ""),
           ("jaeger", "jaeger (16686)", ""), ("mailpit", "mailpit (18025)", "SMTP 11025")]
    for i, (ident, name, desc) in enumerate(obs):
        kind = "external" if ident == "mailpit" else "container"
        c.box(ident, 1130 + 370 * (i % 3), 1200 + 120 * (i // 3), 340, 100, name, "Container", desc, kind=kind, size=16)

    straight_h(c, "user", "web", 220, ("Запросы",), ax=270, both=True)
    straight_v(c, "web", "gateway", 600, ("Запросы", "[HTTP]"), both=True)
    straight_h(c, "gateway", "svc", 545, ("Команды", "[gRPC]"), both=True)
    straight_h(c, "svc", "data", 545, ("SQL, события,", "задачи", "[Postgres, Kafka, AMQP]"), ax=1615)
    straight_v(c, "app", "btc", 640, ("Блоки, broadcast", "[JSON-RPC]"), at_y=1085, both=True)
    straight_v(c, "app", "eth", 880, ("Блоки, broadcast", "[JSON-RPC]"), at_y=1085, both=True)
    straight_v(c, "app", "otel", 1400, ("Метрики, трассы, логи", "[OTLP]"), at_y=1085, dashed=True)

    c.legend(320, 1590, [
        ("person", "Пользователь [Person]"),
        ("node", "Узел развёртывания: хост, группа compose [Deployment Node]"),
        ("container", "Контейнер Tickless [Container]"),
        ("external", "Внешняя система, только для стенда"),
        ("line", "Поток данных"),
        ("dashed", "Телеметрия OTLP"),
    ], width=620)
    return c.write(f"{OUT}/c4-deployment.drawio.svg")


if __name__ == "__main__":
    for build in (context, containers, deployment):
        print(build().relative_to(ROOT))
