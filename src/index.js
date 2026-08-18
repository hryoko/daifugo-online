// ---------- カードロジック（サーバー側が正） ----------
const SUITS = ["S", "H", "D", "C"];
const RANKS = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]; // 15=2, 14=A
const JOKER_RANK = 16;
const RANK_LABEL = (r) => ({ 11: "J", 12: "Q", 13: "K", 14: "A", 15: "2", 16: "Joker" }[r] || String(r));

const DEFAULT_RULES = {
  revolution: true,
  eightCut: true,
  suitLock: false,
  spade3Return: false,
  kaidan: false,
  jokerCount: 0, // 0, 1, 2
  forbiddenAgari: { two: false, eight: false, joker: false, spade3: false },
  miyakoOchi: false,
};

function buildDeck(jokerCount) {
  const deck = [];
  for (const s of SUITS) for (const r of RANKS) deck.push({ id: `${s}-${r}`, suit: s, rank: r });
  for (let i = 0; i < jokerCount; i++) deck.push({ id: `JOKER-${i}`, suit: "JOKER", rank: JOKER_RANK });
  return deck;
}
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
function strength(rank, revolution) {
  return revolution ? -rank : rank;
}
function sortHand(hand, revolution) {
  return [...hand].sort((a, b) => {
    const sa = a.suit === "JOKER" ? 999 : strength(a.rank, revolution);
    const sb = b.suit === "JOKER" ? 999 : strength(b.rank, revolution);
    return sa - sb;
  });
}
function nextActiveIndex(order, players, fromIndex) {
  for (let i = 1; i <= order.length; i++) {
    const idx = (fromIndex + i) % order.length;
    const p = players.find((pl) => pl.id === order[idx]);
    if (p && !p.finished) return idx;
  }
  return fromIndex;
}

// ---------- Durable Object：部屋ごとのゲーム状態 ----------
export class DaifugoRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map(); // ws -> playerId
    this.room = null;
  }

  async ensureLoaded() {
    if (!this.room) {
      this.room = (await this.state.storage.get("room")) || null;
    }
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    await this.ensureLoaded();
    this.sessions.set(server, null);

    server.addEventListener("message", async (evt) => {
      let msg;
      try {
        msg = JSON.parse(evt.data);
      } catch {
        return;
      }
      try {
        await this.handleMessage(server, msg);
      } catch (e) {
        server.send(JSON.stringify({ type: "error", message: "サーバーエラーが発生しました" }));
      }
    });
    server.addEventListener("close", () => this.sessions.delete(server));

    return new Response(null, { status: 101, webSocket: client });
  }

  async persistAndBroadcast() {
    await this.state.storage.put("room", this.room);
    for (const [ws, pid] of this.sessions.entries()) {
      try {
        ws.send(JSON.stringify({ type: "state", room: this.sanitize(pid) }));
      } catch {
        this.sessions.delete(ws);
      }
    }
  }

  sanitize(forPlayerId) {
    if (!this.room) return null;
    return {
      ...this.room,
      players: this.room.players.map((p) => (p.id === forPlayerId ? p : { ...p, hand: undefined })),
    };
  }

  clearField() {
    this.room.field = null;
    this.room.suitLockActive = null;
    this.room.lastSingleSuit = null;
  }

  async handleMessage(ws, msg) {
    const { type, playerId } = msg;

    if (type === "create") {
      this.room = {
        code: msg.code,
        status: "waiting",
        hostId: playerId,
        players: [{ id: playerId, name: msg.name, hand: [], handCount: 0, finished: false, finishOrder: null }],
        order: [],
        field: null,
        suitLockActive: null,
        lastSingleSuit: null,
        lastPlayerId: null,
        currentTurnIndex: 0,
        passStreak: 0,
        revolution: false,
        rules: { ...DEFAULT_RULES, forbiddenAgari: { ...DEFAULT_RULES.forbiddenAgari } },
        previousDaifugoId: null,
        demotedPlayerId: null,
        log: [`${msg.name} が部屋を作成しました`],
      };
      this.sessions.set(ws, playerId);
      await this.persistAndBroadcast();
      return;
    }

    await this.ensureLoaded();
    if (!this.room) {
      ws.send(JSON.stringify({ type: "error", message: "部屋が見つかりません" }));
      return;
    }

    if (type === "join") {
      this.sessions.set(ws, playerId);
      if (!this.room.players.some((p) => p.id === playerId)) {
        if (this.room.status !== "waiting") {
          ws.send(JSON.stringify({ type: "error", message: "すでにゲームが始まっています" }));
          return;
        }
        if (this.room.players.length >= 6) {
          ws.send(JSON.stringify({ type: "error", message: "満員です（最大6人）" }));
          return;
        }
        this.room.players.push({ id: playerId, name: msg.name, hand: [], handCount: 0, finished: false, finishOrder: null });
        this.room.log.push(`${msg.name} が参加しました`);
      }
      await this.persistAndBroadcast();
      return;
    }

    if (type === "setRules") {
      if (playerId !== this.room.hostId) {
        ws.send(JSON.stringify({ type: "error", message: "ホストだけがルールを変更できます" }));
        return;
      }
      if (this.room.status !== "waiting") {
        ws.send(JSON.stringify({ type: "error", message: "ゲーム開始後はルールを変更できません" }));
        return;
      }
      const incoming = msg.rules || {};
      this.room.rules = {
        ...this.room.rules,
        ...incoming,
        forbiddenAgari: { ...this.room.rules.forbiddenAgari, ...(incoming.forbiddenAgari || {}) },
      };
      this.room.log.push("ホストがルールを変更しました");
      await this.persistAndBroadcast();
      return;
    }

    if (type === "start") {
      if (this.room.players.length < 3) {
        ws.send(JSON.stringify({ type: "error", message: "3人以上必要です" }));
        return;
      }
      const rules = this.room.rules;
      const deck = shuffle(buildDeck(rules.jokerCount));
      const order = this.room.players.map((p) => p.id);
      const hands = Object.fromEntries(order.map((id) => [id, []]));
      deck.forEach((card, i) => hands[order[i % order.length]].push(card));
      this.room.players = this.room.players.map((p) => ({
        ...p,
        hand: sortHand(hands[p.id], false),
        handCount: hands[p.id].length,
        finished: false,
        finishOrder: null,
      }));
      this.room.order = order;
      this.room.status = "playing";
      this.room.field = null;
      this.room.suitLockActive = null;
      this.room.lastSingleSuit = null;
      this.room.lastPlayerId = order[0];
      this.room.currentTurnIndex = 0;
      this.room.passStreak = 0;
      this.room.revolution = false;
      this.room.demotedPlayerId = null;
      this.room.log = ["ゲーム開始！"];
      await this.persistAndBroadcast();
      return;
    }

    if (type === "play") {
      const result = this.applyPlay(playerId, msg.cards);
      if (!result.ok) {
        ws.send(JSON.stringify({ type: "error", message: result.message }));
        return;
      }
      await this.persistAndBroadcast();
      return;
    }

    if (type === "pass") {
      const result = this.applyPass(playerId);
      if (!result.ok) {
        ws.send(JSON.stringify({ type: "error", message: result.message }));
        return;
      }
      await this.persistAndBroadcast();
      return;
    }

    if (type === "rematch") {
      this.room.status = "waiting";
      this.room.players = this.room.players.map((p) => ({
        ...p,
        hand: [],
        handCount: 0,
        finished: false,
        finishOrder: null,
      }));
      this.room.field = null;
      this.room.suitLockActive = null;
      this.room.lastSingleSuit = null;
      this.room.log = ["再戦の準備中..."];
      await this.persistAndBroadcast();
      return;
    }
  }

  // 出されたカード群がどんな「役」かを判定する
  classify(cards, rules) {
    const jokers = cards.filter((c) => c.suit === "JOKER");
    const reals = cards.filter((c) => c.suit !== "JOKER");
    if (reals.length === 0 && jokers.length > 0) {
      return { kind: "pureJoker", count: cards.length };
    }
    if (rules.kaidan && jokers.length === 0 && reals.length >= 3) {
      const suits = new Set(reals.map((c) => c.suit));
      const ranks = [...reals.map((c) => c.rank)].sort((a, b) => a - b);
      const consecutive = ranks.every((r, i) => i === 0 || r === ranks[i - 1] + 1);
      if (suits.size === 1 && consecutive) {
        return { kind: "stairs", suit: reals[0].suit, startRank: ranks[0], count: reals.length };
      }
    }
    if (reals.length > 0 && reals.every((c) => c.rank === reals[0].rank)) {
      return { kind: "set", rank: reals[0].rank, count: cards.length };
    }
    return null;
  }

  applyPlay(playerId, cards) {
    const r = this.room;
    const rules = r.rules;
    if (r.status !== "playing") return { ok: false, message: "ゲーム中ではありません" };
    if (r.order[r.currentTurnIndex] !== playerId) return { ok: false, message: "あなたの番ではありません" };
    if (!cards || cards.length === 0) return { ok: false, message: "カードを選んでください" };

    const meIdx = r.players.findIndex((p) => p.id === playerId);
    const hand = r.players[meIdx].hand;
    for (const c of cards) {
      if (!hand.some((h) => h.id === c.id)) return { ok: false, message: "手札にないカードです" };
    }

    const play = this.classify(cards, rules);
    if (!play) return { ok: false, message: "出せる組み合わせではありません" };

    const isSpade3Return =
      rules.spade3Return &&
      r.field &&
      r.field.kind === "pureJoker" &&
      play.kind === "set" &&
      play.count === 1 &&
      cards[0].suit === "S" &&
      cards[0].rank === 3;

    if (!isSpade3Return) {
      if (play.kind === "pureJoker") {
        if (r.field && r.field.count !== play.count) return { ok: false, message: "枚数を合わせてください" };
      } else if (play.kind === "stairs") {
        if (r.field) {
          if (r.field.kind !== "stairs" || r.field.count !== play.count)
            return { ok: false, message: "同じ枚数の階段を出してください" };
          if (strength(play.startRank, r.revolution) <= strength(r.field.startRank, r.revolution))
            return { ok: false, message: "場より強い階段を出してください" };
        }
      } else {
        if (r.field) {
          if (r.field.kind !== "set" || r.field.count !== play.count)
            return { ok: false, message: "同じ役・同じ枚数で出してください" };
          if (strength(play.rank, r.revolution) <= strength(r.field.rank, r.revolution))
            return { ok: false, message: "場より強いカードを出してください" };
        }
        if (rules.suitLock && r.suitLockActive && play.count === 1) {
          if (cards[0].suit !== r.suitLockActive)
            return { ok: false, message: `スート縛り中：${r.suitLockActive}のカードしか出せません` };
        }
      }
    }

    const newHand = hand.filter((c) => !cards.some((s) => s.id === c.id));
    if (newHand.length === 0) {
      const fa = rules.forbiddenAgari;
      const hasJoker = cards.some((c) => c.suit === "JOKER");
      const hasTwo = cards.some((c) => c.suit !== "JOKER" && c.rank === 15);
      const hasEight = cards.some((c) => c.suit !== "JOKER" && c.rank === 8);
      const isSpade3Single = cards.length === 1 && cards[0].suit === "S" && cards[0].rank === 3;
      if ((fa.joker && hasJoker) || (fa.two && hasTwo) || (fa.eight && hasEight) || (fa.spade3 && isSpade3Single)) {
        return { ok: false, message: "反則上がりです。そのカードでは上がれません" };
      }
    }

    r.players[meIdx].hand = newHand;
    r.players[meIdx].handCount = newHand.length;

    const isEightCut = rules.eightCut && play.kind === "set" && cards.every((c) => c.rank === 8);
    if (rules.revolution && play.kind === "set" && play.count === 4) r.revolution = !r.revolution;

    let justFinished = false;
    if (newHand.length === 0) {
      justFinished = true;
      r.players[meIdx].finished = true;
      r.players[meIdx].finishOrder = r.players.filter((p) => p.finished).length;
    }

    const label =
      play.kind === "pureJoker" ? "Joker" : play.kind === "stairs" ? `${RANK_LABEL(play.startRank)}〜階段` : RANK_LABEL(play.rank);
    r.log.push(
      `${r.players[meIdx].name} が ${label} を${cards.length}枚出した${isEightCut ? "（8切り）" : isSpade3Return ? "（スペ3返し）" : ""}`
    );

    const activeCount = r.players.filter((p) => !p.finished).length;
    if (activeCount <= 1) {
      const lastOne = r.players.find((p) => !p.finished);
      if (lastOne) {
        lastOne.finished = true;
        lastOne.finishOrder = r.players.length;
      }
      if (rules.miyakoOchi) {
        r.demotedPlayerId = r.previousDaifugoId || null;
        const winner = r.players.find((p) => p.finishOrder === 1);
        r.previousDaifugoId = winner ? winner.id : null;
      } else {
        r.demotedPlayerId = null;
      }
      r.status = "finished";
      r.field = null;
      return { ok: true };
    }

    r.lastPlayerId = playerId;
    r.passStreak = 0;

    if (isSpade3Return) {
      this.clearField();
      if (justFinished) r.currentTurnIndex = nextActiveIndex(r.order, r.players, r.currentTurnIndex);
    } else if (isEightCut) {
      this.clearField();
      if (justFinished) r.currentTurnIndex = nextActiveIndex(r.order, r.players, r.currentTurnIndex);
    } else {
      if (play.kind === "set") r.field = { kind: "set", rank: play.rank, count: play.count };
      else if (play.kind === "stairs") r.field = { kind: "stairs", suit: play.suit, startRank: play.startRank, count: play.count };
      else r.field = { kind: "pureJoker", count: play.count };

      if (rules.suitLock && play.kind === "set" && play.count === 1) {
        const suit = cards[0].suit;
        if (r.lastSingleSuit === suit) r.suitLockActive = suit;
        else {
          r.suitLockActive = null;
          r.lastSingleSuit = suit;
        }
      }
      r.currentTurnIndex = nextActiveIndex(r.order, r.players, r.currentTurnIndex);
    }
    return { ok: true };
  }

  applyPass(playerId) {
    const r = this.room;
    if (r.status !== "playing") return { ok: false, message: "ゲーム中ではありません" };
    if (r.order[r.currentTurnIndex] !== playerId) return { ok: false, message: "あなたの番ではありません" };
    if (!r.field) return { ok: false, message: "最初の1手はパスできません" };

    r.passStreak += 1;
    r.log.push(`${r.players.find((p) => p.id === playerId).name} がパス`);
    const activeCount = r.players.filter((p) => !p.finished).length;

    if (r.passStreak >= activeCount - 1) {
      const lastIdx = r.order.indexOf(r.lastPlayerId);
      this.clearField();
      r.passStreak = 0;
      r.currentTurnIndex = nextActiveIndex(r.order, r.players, lastIdx >= 0 ? lastIdx - 1 : r.currentTurnIndex);
    } else {
      r.currentTurnIndex = nextActiveIndex(r.order, r.players, r.currentTurnIndex);
    }
    return { ok: true };
  }
}

// ---------- Worker エントリーポイント ----------
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/api\/room\/([A-Za-z0-9]{4})\/ws$/);
    if (match) {
      const code = match[1].toUpperCase();
      const id = env.ROOM.idFromName(code);
      const stub = env.ROOM.get(id);
      return stub.fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};
