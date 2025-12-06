import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import crypto from 'crypto';
import { promises as fs } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';
import { createClient } from 'redis';
import bullmqPkg from 'bullmq';
const { Queue, Worker } = bullmqPkg || {};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_FILE = path.join(__dirname, 'data', 'users.json');
const CHAT_FILE = path.join(__dirname, 'data', 'chat.json');
const POLL_FILE = path.join(__dirname, 'data', 'polls.json');
const TEAM_CACHE_FILE = path.join(__dirname, 'data', 'teams.json');
const PLAYER_CACHE_FILE = path.join(__dirname, 'data', 'players.json');
const VIDEOS_FILE = path.join(__dirname, 'data', 'videos.json');
const ANALYTICS_FILE = path.join(__dirname, 'data', 'analytics.json');
const NOTIFY_FILE = path.join(__dirname, 'data', 'notifications.json');
const FANPOST_FILE = path.join(__dirname, 'data', 'fanposts.json');
const PREDICTIONS_FILE = path.join(__dirname, 'data', 'predictions.json');
const RECAPS_FILE = path.join(__dirname, 'data', 'recaps.json');
const ELO_FILE = path.join(__dirname, 'data', 'elo.json');
const CLIPS_FILE = path.join(__dirname, 'data', 'clips.json');
const DEVICE_PAIR_FILE = path.join(__dirname, 'data', 'devicepairs.json');
const SUBS_FILE = path.join(__dirname, 'data', 'subscriptions.json');
const CREATOR_CONTENT_FILE = path.join(__dirname, 'data', 'creatorContent.json');
const PROVIDER_CONFIG_FILE = path.join(__dirname, 'provider.config.json');
const HISTORY_FILE = path.join(__dirname, 'data', 'history.json');
const ANALYTICS_ROLLUP_FILE = path.join(__dirname, 'data', 'analyticsSummary.json');
const CLIP_INDEX_FILE = path.join(__dirname, 'data', 'clipIndex.json');
const SHOT_CACHE_TTL = 5 * 60 * 1000;
const DRIVE_CACHE_TTL = 2 * 60 * 1000;
const COURT_HALF_WIDTH = 250;
const COURT_LENGTH = 470;
const DEFAULT_REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const CHAT_CHANNEL = 'streamdashboard:chat';
const POLL_CHANNEL = 'streamdashboard:poll';
const CHAT_HISTORY_LIMIT = 120;
const QUEUE_NAMES={
  recap:'streamdashboard-recap',
  analytics:'streamdashboard-analytics',
  clips:'streamdashboard-clips'
};
const SCOREBOARD_ROLLOVER_HOUR = 5; // eastern
const BASE_ELO_RATING = 1500;
const DEFAULT_K_FACTOR = 18;
const TREND_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const HISTORY_LIMIT = 30;
const DEVICE_PAIR_TTL = 5 * 60 * 1000;
const PAIR_CODE_CHARS = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
const PAIR_CODE_LENGTH = 6;
const LEAGUE_K_FACTORS = {
  NFL:24,
  NCAAF:24,
  UFC:28
};
const NORMALIZE_MIN = 1300;
const NORMALIZE_MAX = 1900;
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || '';
const SPORT_ENDPOINTS = {
  mlb: d => `https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=${d}`,
  nba: d => `https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=${d}`,
  nhl: d => `https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=${d}`,
  nfl: d => `https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=${d}`,
  ncaaf: d => `https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=${d}`,
  ncaam: d => `https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates=${d}`,
  ufc: () => `https://site.api.espn.com/apis/personalized/v2/scoreboard/header?sport=mma&league=ufc`
};
const STANDINGS_ENDPOINTS = {
  mlb:'https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings',
  nba:'https://site.api.espn.com/apis/v2/sports/basketball/nba/standings',
  nhl:'https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings',
  nfl:'https://site.api.espn.com/apis/v2/sports/football/nfl/standings'
};
const NEWS_ENDPOINTS = {
  mlb:'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news',
  nba:'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/news',
  nhl:'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/news',
  nfl:'https://site.api.espn.com/apis/site/v2/sports/football/nfl/news',
  ncaaf:'https://site.api.espn.com/apis/site/v2/sports/football/college-football/news',
  ncaam:'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/news',
  ufc:'https://site.api.espn.com/apis/site/v2/sports/mma/ufc/news'
};
const TEAM_ENDPOINT_TEMPLATES=[
  'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/',
  'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/',
  'https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/',
  'https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/',
  'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/',
  'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/'
];
const PLAYER_ENDPOINT_TEMPLATES=[
  'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/',
  'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/',
  'https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/',
  'https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/',
  'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/athletes/',
  'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/'
];
const PROFILE_TTL = 6 * 60 * 60 * 1000; // 6 hours
const ANALYTICS_MAX_EVENTS = 2000;
let analyticsFlushTimer = null;
let latestScheduleGames=[];
let latestScheduleStamp=0;

const RIVALRY_MATCHUPS=new Set([
  'BOS|LAL',
  'BOS|NYY',
  'CHI|GB',
  'DAL|PHI',
  'LIV|MCI',
  'MIA|NYK',
  'NJD|NYR',
  'PHI|PIT',
  'TOR|MTL'
]);
const LEAGUE_SEGMENTS={
  NBA:4,
  NCAAM:2,
  NCAAF:4,
  NFL:4,
  NHL:3,
  MLB:9,
  MLS:2,
  UFC:5
};
const SCORE_CACHE_LIMIT=600;
const lastScoreSnapshot=new Map();
const shotChartCache=new Map();
const driveChartCache=new Map();
const SOCIAL_CACHE_TTL = 10 * 60 * 1000;
const HISTORY_CONFIG = {
  nba:{
    standings:'https://site.api.espn.com/apis/v2/sports/basketball/nba/standings',
    scoreboardSlug:'basketball/nba',
    playoffDate:'0610',
    playoffOffset:0
  },
  nfl:{
    standings:'https://site.api.espn.com/apis/v2/sports/football/nfl/standings',
    scoreboardSlug:'football/nfl',
    playoffDate:'0205',
    playoffOffset:1
  },
  nhl:{
    standings:'https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings',
    scoreboardSlug:'hockey/nhl',
    playoffDate:'0605',
    playoffOffset:0
  },
  mlb:{
    standings:'https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings',
    scoreboardSlug:'baseball/mlb',
    playoffDate:'1030',
    playoffOffset:0
  }
};
const DEFAULT_PREFERENCES = {
  leagues: ['mlb','nba','nfl','ncaaf','ncaam','nhl','ufc'],
  provider: 'admin',
  discoveryBias: 'mixed'
};

function normalizeKey(value){
  return (value||'').toUpperCase().replace(/[^A-Z]/g,'');
}
function rivalryKey(game){
  const home=normalizeKey(game.home?.short||game.home?.name);
  const away=normalizeKey(game.away?.short||game.away?.name);
  return [home,away].sort().join('|');
}
function isRivalryGame(game){
  const key=rivalryKey(game);
  return key.trim().length>1 && RIVALRY_MATCHUPS.has(key);
}
function parseScoreValue(value){
  const num=Number(value);
  return Number.isFinite(num)?num:0;
}
function computeGameProgress(game){
  const league=String(game.league||'').toUpperCase();
  const segments=LEAGUE_SEGMENTS[league] || 4;
  const period=Number(game.status?.period);
  if(!period || segments<=1) return 0;
  const base=(period-1)/(segments-1);
  return Math.max(0, Math.min(1, base));
}
function pruneScoreCache(){
  if(lastScoreSnapshot.size<=SCORE_CACHE_LIMIT) return;
  const entries=[...lastScoreSnapshot.entries()].sort((a,b)=>a[1].ts-b[1].ts);
  const excess=entries.length-SCORE_CACHE_LIMIT;
  for(let i=0;i<excess;i++){
    lastScoreSnapshot.delete(entries[i][0]);
  }
}
function computeExcitementScore(game, favoritesSet=new Set()){
  const state=(game.status?.state||'').toLowerCase();
  const homeScore=parseScoreValue(game.home?.score);
  const awayScore=parseScoreValue(game.away?.score);
  const diff=Math.abs(homeScore-awayScore);
  const closeness=1-Math.min(diff,25)/25;
  const progress=state==='in'?computeGameProgress(game):(state==='post'?1:0);
  const prev=lastScoreSnapshot.get(game.id);
  const momentum=state==='in' && prev && (prev.home!==homeScore || prev.away!==awayScore) ? 1 : 0;
  const homeId=game.home?.id ? String(game.home.id) : null;
  const awayId=game.away?.id ? String(game.away.id) : null;
  const favoriteBonus=(homeId && favoritesSet.has(homeId)) || (awayId && favoritesSet.has(awayId)) ? 0.15 : 0;
  const rivalryBonus=isRivalryGame(game)?0.1:0;
  let excitement=0.3;
  if(state==='in'){
    excitement=0.35 + closeness*0.35 + progress*0.2 + momentum*0.15;
  }else if(state==='post'){
    excitement=0.25 + closeness*0.3 + momentum*0.1;
  }else{
    excitement=0.25 + favoriteBonus*0.4;
  }
  excitement+=favoriteBonus+rivalryBonus;
  const clamped=Math.max(0, Math.min(1, Number(excitement.toFixed(3))));
  lastScoreSnapshot.set(game.id,{ home:homeScore, away:awayScore, ts:Date.now() });
  pruneScoreCache();
  return clamped || 0.3;
}

function getKFactor(league){
  const key=(league||'').toUpperCase();
  return LEAGUE_K_FACTORS[key] || DEFAULT_K_FACTOR;
}

function getEloEntry(team, game){
  const teamId=String(team?.id||'');
  if(!teamId) return null;
  if(!eloRatings.has(teamId)){
    eloRatings.set(teamId,{
      id:teamId,
      name:team?.name || team?.short || `Team ${teamId}`,
      league:(game?.league||team?.league||'').toUpperCase(),
      logo:team?.logo||'',
      rating:BASE_ELO_RATING,
      wins:0,
      losses:0,
      ties:0,
      games:0,
      history:[{ ts:Date.now(), rating:BASE_ELO_RATING }]
    });
  }
  const entry=eloRatings.get(teamId);
  entry.wins=entry.wins||0;
  entry.losses=entry.losses||0;
  entry.ties=entry.ties||0;
  entry.games=entry.games||0;
  if(!Array.isArray(entry.history)) entry.history=[];
  if(team?.name) entry.name=team.name;
  if(team?.short) entry.abbr=team.short;
  if(team?.logo && !entry.logo) entry.logo=team.logo;
  if(game?.league) entry.league=(game.league||entry.league||'').toUpperCase();
  return entry;
}

function updateEloRatingsForGame(game){
  const state=(game.status?.state||'').toLowerCase();
  if(state!=='post') return false;
  if(processedEloGames.has(game.id)) return false;
  const home=game.home;
  const away=game.away;
  if(!home?.id || !away?.id) return false;
  const homeEntry=getEloEntry(home, game);
  const awayEntry=getEloEntry(away, game);
  if(!homeEntry || !awayEntry) return false;
  const homeScore=parseScoreValue(home.score);
  const awayScore=parseScoreValue(away.score);
  const actualHome=homeScore>awayScore ? 1 : homeScore<awayScore ? 0 : 0.5;
  const expectedHome=1/(1+Math.pow(10,(awayEntry.rating-homeEntry.rating)/400));
  const expectedAway=1-expectedHome;
  const k=getKFactor(game.league);
  homeEntry.rating=roundRating(homeEntry.rating + k*(actualHome-expectedHome));
  awayEntry.rating=roundRating(awayEntry.rating + (1-actualHome - expectedAway)*k);
  homeEntry.games+=1;
  awayEntry.games+=1;
  if(actualHome===1){
    homeEntry.wins+=1;
    awayEntry.losses+=1;
  }else if(actualHome===0){
    awayEntry.wins+=1;
    homeEntry.losses+=1;
  }else{
    homeEntry.ties=(homeEntry.ties||0)+1;
    awayEntry.ties=(awayEntry.ties||0)+1;
  }
  recordEloHistory(homeEntry);
  recordEloHistory(awayEntry);
  processedEloGames.add(game.id);
  return true;
}

function recordEloHistory(entry){
  if(!Array.isArray(entry.history)) entry.history=[];
  entry.history.push({ ts:Date.now(), rating:roundRating(entry.rating) });
  if(entry.history.length>HISTORY_LIMIT){
    entry.history=entry.history.slice(-HISTORY_LIMIT);
  }
}

function roundRating(value){
  return Number((value||0).toFixed(2));
}

function buildPowerRankingSnapshot(){
  const rows=[...eloRatings.values()].map(enrichPowerEntry).sort((a,b)=>b.rating-a.rating);
  const leaderboard=rows.slice(0,15);
  const hot=rows.filter(entry=>entry.trend>0).sort((a,b)=>b.trend-a.trend).slice(0,8);
  const byLeague={};
  rows.forEach(entry=>{
    const league=entry.league || 'OTHER';
    if(!byLeague[league]) byLeague[league]=[];
    if(byLeague[league].length<5){
      byLeague[league].push(entry);
    }
  });
  return {
    updatedAt:eloUpdatedAt,
    leaderboard,
    hot,
    byLeague
  };
}

function enrichPowerEntry(entry){
  const history=Array.isArray(entry.history)
    ? entry.history.map(pt=>({ ts:Number(pt.ts)||Date.now(), rating:Number(pt.rating)||BASE_ELO_RATING }))
    : [];
  const latest=history[history.length-1] || { rating:entry.rating, ts:Date.now() };
  const baselineTs=latest.ts - TREND_WINDOW_MS;
  let baseline=history.find(pt=>pt.ts>=baselineTs) || history[0] || latest;
  if(!baseline) baseline=latest;
  const trend=roundRating((entry.rating||BASE_ELO_RATING) - (baseline.rating||BASE_ELO_RATING));
  return {
    ...entry,
    rating:roundRating(entry.rating||BASE_ELO_RATING),
    record:formatRecord(entry),
    trend,
    normalized:normalizePowerIndex(entry.rating||BASE_ELO_RATING)
  };
}

function formatRecord(entry){
  const wins=entry.wins||0;
  const losses=entry.losses||0;
  const ties=entry.ties||0;
  return ties ? `${wins}-${losses}-${ties}` : `${wins}-${losses}`;
}

function normalizePowerIndex(rating){
  const clamped=Math.max(NORMALIZE_MIN, Math.min(NORMALIZE_MAX, rating));
  const pct=(clamped-NORMALIZE_MIN)/(NORMALIZE_MAX-NORMALIZE_MIN);
  return Math.round(pct*100);
}

async function getTeamProfile(teamId){
  const key=String(teamId);
  if(!key) return null;
  const cached=teamProfileCache.get(key);
  if(cached && Date.now()-cached.ts<PROFILE_TTL){
    return cached.data;
  }
  const profile=await fetchTeamProfileFromSource(key);
  const payload=profile || { id:key, name:`Team ${key}`, summary:'Info unavailable', roster:[] };
  teamProfileCache.set(key,{ data:payload, ts:Date.now() });
  await persistProfileCache(TEAM_CACHE_FILE, teamProfileCache);
  return payload;
}

async function getPlayerProfile(playerId){
  const key=String(playerId);
  if(!key) return null;
  const cached=playerProfileCache.get(key);
  if(cached && Date.now()-cached.ts<PROFILE_TTL){
    return cached.data;
  }
  const profile=await fetchPlayerProfileFromSource(key);
  const payload=profile || { id:key, fullName:`Player ${key}`, bio:'Details unavailable', stats:[] };
  playerProfileCache.set(key,{ data:payload, ts:Date.now() });
  await persistProfileCache(PLAYER_CACHE_FILE, playerProfileCache);
  return payload;
}

async function fetchTeamProfileFromSource(teamId){
  for(const template of TEAM_ENDPOINT_TEMPLATES){
    try{
      const data=await fetchJSON(`${template}${teamId}`);
      if(data?.team){
        return normalizeTeamProfile(data.team);
      }
      if(data?.athletes || data?.teamId){
        return normalizeTeamProfile(data);
      }
    }catch(err){}
  }
  return null;
}

async function fetchPlayerProfileFromSource(playerId){
  for(const template of PLAYER_ENDPOINT_TEMPLATES){
    try{
      const data=await fetchJSON(`${template}${playerId}`);
      if(data?.athlete){
        return normalizePlayerProfile(data.athlete);
      }
      if(data?.athletes){
        return normalizePlayerProfile(data.athletes[0]);
      }
    }catch(err){}
  }
  return null;
}

function normalizeTeamProfile(raw){
  const team=raw?.team || raw;
  if(!team) return null;
  const logos=team.logos || (team.logo?[{ href:team.logo }]:[]);
  const roster=(team.athletes||team.team?.athletes||[]).map(ath=>({
    id:String(ath.id||ath.athleteId||''),
    fullName:ath.fullName||ath.displayName||ath.name,
    position:ath.position?.abbreviation||ath.position||'',
    headshot:ath.headshot?.href||ath.photo||'',
    jersey:ath.jersey||'',
    team:{
      id:String(team.id||team.teamId||''),
      name:team.displayName||team.name
    }
  })).filter(player=>player.id);
  return {
    id:String(team.id||team.teamId||''),
    name:team.displayName||team.name||team.shortDisplayName||'Team',
    shortName:team.shortDisplayName||team.abbreviation||'',
    logo:logos?.[0]?.href||'',
    record:team.record?.items?.[0]?.summary || team.standingSummary || '',
    color:team.color || team.alternateColor || '',
    venue:team.franchise?.venue?.fullName || team.venue?.fullName || '',
    nextEvent:team.nextEvent?.[0]?.shortName || '',
    summary:team.franchise?.description || team.description || 'No summary available.',
    league:team.sport?.name || team.slug || '',
    rankings:team.rankings || [],
    stats:team.statistics || team.stats || [],
    injuries:team.injuries || [],
    links:(team.links||[]).map(l=>({ text:l.text||l.shortText||'Link', href:l.href||'#' })),
    roster:roster.slice(0,25),
    lastUpdated:Date.now()
  };
}

function normalizePlayerProfile(athlete){
  if(!athlete) return null;
  const team=athlete.team || athlete.proTeam || {};
  const stats=(athlete.statistics?.splits?.categories||[]).map(cat=>({
    name:cat.displayName || cat.name,
    stats:(cat.stats||[]).map(stat=>({
      name:stat.displayName || stat.name,
      value:stat.displayValue || stat.value
    }))
  }));
  return {
    id:String(athlete.id || athlete.athleteId || ''),
    fullName:athlete.fullName || athlete.displayName || athlete.shortName || 'Player',
    headshot:athlete.headshot?.href || athlete.photo || '',
    position:athlete.position?.abbreviation || athlete.position?.displayName || '',
    team:team ? { id:String(team.id||''), name:team.displayName||team.name||team.shortDisplayName||'' } : null,
    bio:athlete.displayBio || athlete.shortBio || '',
    age:athlete.age || null,
    height:athlete.displayHeight || '',
    weight:athlete.displayWeight || '',
    stats,
    lastUpdated:Date.now()
  };
}

function getVideosForGame(gameId){
  const key=String(gameId);
  return videoCatalog.filter(video=>String(video.gameId)===key);
}

function getLatestVideos(limit=12){
  return [...videoCatalog].sort((a,b)=>(Number(b.timestamp)||0)-(Number(a.timestamp)||0)).slice(0,limit);
}

function isAdminRequest(req){
  const adminKey=process.env.ADMIN_METRICS_TOKEN;
  if(adminKey){
    return req.headers['x-admin-key']===adminKey;
  }
  return Boolean(resolveUserFromRequest(req));
}

function buildAnalyticsSummary(){
  const counts={};
  const topGames=new Map();
  const chatCounts=new Map();
  const pollCounts=new Map();
  analyticsEvents.forEach(evt=>{
    counts[evt.type]=(counts[evt.type]||0)+1;
    if(evt.gameId){
      const entry=topGames.get(evt.gameId)||{ gameId:evt.gameId, count:0 };
      entry.count+=1;
      topGames.set(evt.gameId, entry);
    }
    if(evt.type==='chat:message' && evt.gameId){
      chatCounts.set(evt.gameId,(chatCounts.get(evt.gameId)||0)+1);
    }
    if(evt.type.startsWith('poll') && evt.gameId){
      pollCounts.set(evt.gameId,(pollCounts.get(evt.gameId)||0)+1);
    }
  });
  return {
    eventCounts:counts,
    topGames:[...topGames.values()].sort((a,b)=>b.count-a.count).slice(0,5),
    chatActivity:[...chatCounts.entries()].map(([gameId,count])=>({ gameId,count })).sort((a,b)=>b.count-a.count).slice(0,5),
    pollActivity:[...pollCounts.entries()].map(([gameId,count])=>({ gameId,count })).sort((a,b)=>b.count-a.count).slice(0,5),
    recentEvents:analyticsEvents.slice(-50).reverse()
  };
}

async function persistAnalyticsSummary(summary){
  analyticsRollupCache={ summary, generatedAt:Date.now() };
  await fs.writeFile(ANALYTICS_ROLLUP_FILE, JSON.stringify(analyticsRollupCache, null, 2));
}

async function ensureFile(filePath, fallback){
  try{
    await fs.access(filePath);
  }catch{
    await fs.mkdir(path.dirname(filePath), { recursive:true });
    await fs.writeFile(filePath, JSON.stringify(fallback, null, 2));
  }
}
await ensureFile(DATA_FILE, { users:[], profiles:{} });
await ensureFile(CHAT_FILE, {});
await ensureFile(POLL_FILE, {});
await ensureFile(TEAM_CACHE_FILE, {});
await ensureFile(PLAYER_CACHE_FILE, {});
await ensureFile(VIDEOS_FILE, { videos:[] });
await ensureFile(ANALYTICS_FILE, { events:[] });
await ensureFile(NOTIFY_FILE, { subscriptions:{} });
await ensureFile(FANPOST_FILE, {});
await ensureFile(PREDICTIONS_FILE, { predictions:[], leagues:[] });
await ensureFile(RECAPS_FILE, {});
await ensureFile(ELO_FILE, { teams:{}, processedGames:{}, updatedAt:Date.now() });
await ensureFile(HISTORY_FILE, {});
await ensureFile(CLIPS_FILE, { clips:[] });
await ensureFile(DEVICE_PAIR_FILE, { codes:{} });
await ensureFile(SUBS_FILE, { subs:{} });
await ensureFile(CREATOR_CONTENT_FILE, { creators:[], entries:[] });
await ensureFile(PROVIDER_CONFIG_FILE, { dataProvider:'espn' });
await ensureFile(ANALYTICS_ROLLUP_FILE, { summary:null, generatedAt:0 });
await ensureFile(CLIP_INDEX_FILE, { items:[], updatedAt:0 });

async function searchEspnPlayers(query, limit=10){
  if(!query || !query.trim()) return [];
  const url=new URL('https://site.api.espn.com/apis/search/v2');
  url.searchParams.set('type','athlete');
  url.searchParams.set('limit', String(Math.max(1, Math.min(limit, 25))));
  url.searchParams.set('page','1');
  url.searchParams.set('query', query.trim());
  try{
    const data=await fetchJSON(url.toString());
    const items=(data?.results||[]).filter(item=>(item.type||'').toLowerCase()==='athlete');
    return items.slice(0,limit).map(item=>({
      id:String(item.id||item.uid||''),
      name:item.name || item.fullName || item.shortName || query,
      team:item.team?.displayName || item.team?.name || '',
      position:item.position || '',
      headshot:item.images?.[0]?.url || '',
      url:item.links?.[0]?.href || item.web?.href || '',
      source:'ESPN'
    }));
  }catch(err){
    console.warn('Player search failed', err.message);
    return [];
  }
}

function createEspnProvider(){
  return {
    id:'espn',
    name:'ESPN',
    async getSchedule(params={}){
      const date=params.date || todayStr();
      const leagues=(params.leagues||['mlb','nba','nfl','ncaaf','ncaam','nhl','ufc']).map(lg=>lg.toLowerCase());
      const batches=await Promise.all(leagues.map(lg=>fetchEspnLeague(lg, date)));
      return { games:batches.flat().sort(sortGames), leagues };
    },
    async getGame(gameId){
      if(!gameId) return null;
      const cached=latestScheduleGames.find(g=>String(g.id)===String(gameId));
      if(cached) return cached;
      return null;
    },
    async getGameDetails(gameId){
      return this.getGame(gameId);
    },
    async getStats(params={}){
      const game=await this.getGame(params.gameId);
      return game?.insights || null;
    },
    async getTeam(teamId){
      return await getTeamProfile(teamId);
    },
    async getPlayer(playerId){
      return await getPlayerProfile(playerId);
    },
    async searchPlayers(query, options={}){
      return await searchEspnPlayers(query, options.limit||10);
    }
  };
}

function createFallbackProvider(){
  const placeholder=[];
  return {
    id:'fallback',
    name:'Fallback',
    async getSchedule(){
      return { games:placeholder };
    },
    async getGame(){ return null; },
    async getGameDetails(){ return null; },
    async getStats(){ return null; },
    async getTeam(){ return null; },
    async getPlayer(){ return null; },
    async searchPlayers(){ return []; }
  };
}

const providerFactories={
  espn:createEspnProvider,
  fallback:createFallbackProvider
};

function normalizeSchedulePayload(result){
  if(Array.isArray(result)) return result;
  if(result && Array.isArray(result.games)) return result.games;
  return [];
}

async function fetchScheduleFromProvider(provider, params){
  if(!provider || typeof provider.getSchedule!=='function') return [];
  try{
    const data=await provider.getSchedule(params||{});
    const games=normalizeSchedulePayload(data);
    return Array.isArray(games) ? games : [];
  }catch(err){
    console.warn(`[schedule] Provider ${provider.id||'unknown'} threw error:`, err.message);
    return [];
  }
}

async function initDataProvider(explicitName){
  const config=await loadJson(PROVIDER_CONFIG_FILE) || {};
  const requested=(explicitName || process.env.DATA_PROVIDER || config.dataProvider || 'espn').toLowerCase();
  let factory=providerFactories[requested];
  if(!factory){
    console.warn(`[provider] Unknown provider "${requested}", falling back to ESPN.`);
    factory=providerFactories.espn;
  }
  try{
    const provider=await factory();
    provider.id=provider.id || requested;
    if(typeof provider.getSchedule!=='function'){
      throw new Error('Provider missing getSchedule');
    }
    return provider;
  }catch(err){
    console.error('[provider] FAILED, falling back to ESPN:', err.message);
    const fallback=await providerFactories.espn();
    fallback.id='espn';
    return fallback;
  }
}

async function readData(){
  const raw = await fs.readFile(DATA_FILE, 'utf-8');
  return JSON.parse(raw);
}
async function writeData(data){
  await fs.writeFile(DATA_FILE, JSON.stringify(data, null, 2));
}

async function loadJson(file){
  try{
    const raw=await fs.readFile(file,'utf-8');
    return JSON.parse(raw);
  }catch{
    return null;
  }
}

const chatStore = new Map();
const pollStore = new Map();
const teamProfileCache=new Map();
const playerProfileCache=new Map();
let videoCatalog=[];
const analyticsEvents=[];
const notifyStore=new Map();
const fanPostsStore=new Map();
const socialCache=new Map();
const recapStore=new Map();
let predictionsLog=[];
let leagueStore=[];
let eloRatings=new Map();
let processedEloGames=new Set();
let eloUpdatedAt=Date.now();
let historyStore={};
let clipStore=[];
let creatorProfiles=[];
let creatorEntries=[];
const subscriptionMap=new Map();
const devicePairStore=new Map();
let redisClient=null;
let redisPublisher=null;
let redisSubscriber=null;
let redisEnabled=false;
const socketClients=new Set();
const chatAuthorCounts=new Map();
let analyticsRollupCache=await loadJson(ANALYTICS_ROLLUP_FILE) || { summary:null, generatedAt:0 };
let clipIndexCache=await loadJson(CLIP_INDEX_FILE) || { items:[], updatedAt:0 };
let recapQueue=null;
let analyticsQueue=null;
let clipQueue=null;
let queuesReady=false;
const taskWorkers=[];
let dataProvider = await initDataProvider();

await initRedis();
if(!redisEnabled){
  await loadChatStore();
  await loadPollStore();
  rebuildAuthorCountsFromChats();
}
await loadProfileCache(TEAM_CACHE_FILE, teamProfileCache);
await loadProfileCache(PLAYER_CACHE_FILE, playerProfileCache);
await loadVideoCatalog();
await loadAnalyticsEvents();
await loadFanPosts();
await loadPredictions();
await loadRecaps();
await loadNotifyStore();
await loadEloStore();
await loadHistoryStore();
await loadClips();
await loadDevicePairs();
await loadSubscriptions();
await loadCreatorContent();
await initTaskQueues();

async function loadChatStore(){
  const data=await loadJson(CHAT_FILE) || {};
  Object.entries(data).forEach(([gameId, messages])=>{
    chatStore.set(gameId, Array.isArray(messages)?messages.slice(-100):[]);
  });
}

async function persistChats(){
  const payload={};
  chatStore.forEach((messages, gameId)=>{
    payload[gameId]=messages.slice(-100);
  });
  await fs.writeFile(CHAT_FILE, JSON.stringify(payload, null, 2));
}

function rebuildAuthorCountsFromChats(){
  chatAuthorCounts.clear();
  chatStore.forEach(messages=>{
    (messages||[]).forEach(msg=>{
      if(!msg?.author) return;
      const current=chatAuthorCounts.get(msg.author)||0;
      chatAuthorCounts.set(msg.author, current+1);
    });
  });
}

async function loadPollStore(){
  const data=await loadJson(POLL_FILE) || {};
  Object.entries(data).forEach(([gameId, poll])=>{
    pollStore.set(gameId, poll);
  });
}

async function persistPolls(){
  const payload={};
  pollStore.forEach((poll, gameId)=>{ payload[gameId]=poll; });
  await fs.writeFile(POLL_FILE, JSON.stringify(payload, null, 2));
}

const BAD_WORDS=['shit','damn','fuck'];
const chatRateLimiter=new Map();
const RATE_WINDOW=2000;

function sanitizeMessage(body){
  if(!body) return null;
  const clean=String(body).trim();
  if(!clean) return null;
  const lowered=clean.toLowerCase();
  if(BAD_WORDS.some(word=>lowered.includes(word))) return null;
  return clean.slice(0,280);
}

async function broadcastChat(gameId='global'){
  const messages=await getChatHistory(gameId);
  const payload=JSON.stringify({ type:'chat:update', gameId, messages });
  socketClients.forEach(client=>{
    if(client.readyState===1 && client.room===(gameId||'global')){
      try{ client.send(payload); }catch(err){ console.error('Chat send failed', err.message); }
    }
  });
}

async function broadcastPoll(gameId){
  const poll=await getPollSnapshot(gameId);
  if(!poll) return;
  const payload=JSON.stringify({ type:'poll:update', gameId, poll });
  socketClients.forEach(client=>{
    if(client.readyState===1 && client.room===(gameId||'global')){
      try{ client.send(payload); }catch(err){ console.error('Poll send failed', err.message); }
    }
  });
}

async function pushRoomState(ws, room='global'){
  try{
    const messages=await getChatHistory(room);
    ws.send(JSON.stringify({ type:'chat:update', gameId:room, messages }));
    const poll=await getPollSnapshot(room);
    if(poll){
      ws.send(JSON.stringify({ type:'poll:update', gameId:room, poll }));
    }
  }catch(err){
    console.error('Room state send failed', err.message);
  }
}

function chatHistoryKey(room){
  return `chat:history:${room||'global'}`;
}

async function getChatHistory(gameId='global'){
  const key=gameId||'global';
  if(redisEnabled && redisClient){
    try{
      const rows=await redisClient.lRange(chatHistoryKey(key), -CHAT_HISTORY_LIMIT, -1);
      return rows.map(row=>{
        try{
          return JSON.parse(row);
        }catch{
          return null;
        }
      }).filter(Boolean);
    }catch(err){
      console.error('Chat history fetch failed', err.message);
    }
  }
  return chatStore.get(key)||[];
}

async function appendChatMessage(gameId, msg){
  const key=gameId||'global';
  const entry={ ...msg, id:crypto.randomUUID(), ts:msg.ts||Date.now() };
  if(redisEnabled && redisClient){
    try{
      const storageKey=chatHistoryKey(key);
      await redisClient.rPush(storageKey, JSON.stringify(entry));
      await redisClient.lTrim(storageKey, -CHAT_HISTORY_LIMIT, -1);
      if(redisPublisher){
        await redisPublisher.publish(CHAT_CHANNEL, JSON.stringify({ gameId:key }));
      }
    }catch(err){
      console.error('Redis chat append failed', err.message);
    }
  }else{
    const messages=chatStore.get(key)||[];
    messages.push(entry);
    chatStore.set(key, messages.slice(-CHAT_HISTORY_LIMIT));
    await persistChats();
  }
  if(entry.author){
    chatAuthorCounts.set(entry.author, (chatAuthorCounts.get(entry.author)||0)+1);
    if(redisEnabled && redisClient){
      try{
        await redisClient.hIncrBy('chat:authorCounts', entry.author, 1);
      }catch(err){
        console.error('Author count increment failed', err.message);
      }
    }
  }
  await broadcastChat(key);
  return entry;
}

async function getPollSnapshot(gameId){
  if(redisEnabled && redisClient){
    try{
      const value=await redisClient.hGet('poll:data', gameId);
      return value ? JSON.parse(value) : null;
    }catch(err){
      console.error('Poll fetch failed', err.message);
    }
  }
  return pollStore.get(gameId)||null;
}

async function savePollSnapshot(gameId, poll, { broadcast=true } = {}){
  if(redisEnabled && redisClient){
    try{
      await redisClient.hSet('poll:data', gameId, JSON.stringify(poll));
      if(broadcast && redisPublisher){
        await redisPublisher.publish(POLL_CHANNEL, JSON.stringify({ gameId }));
      }
    }catch(err){
      console.error('Poll persist failed', err.message);
    }
  }else{
    pollStore.set(gameId, poll);
    await persistPolls();
  }
  if(broadcast){
    await broadcastPoll(gameId);
  }
}

async function initRedis(){
  const redisUrl=DEFAULT_REDIS_URL;
  try{
    redisClient=createClient({ url:redisUrl });
    redisPublisher=redisClient.duplicate();
    redisSubscriber=redisClient.duplicate();
    [redisClient, redisPublisher, redisSubscriber].forEach(client=>{
      client.on('error',err=>console.error('[redis]', err.message));
    });
    await redisClient.connect();
    await redisPublisher.connect();
    await redisSubscriber.connect();
    redisEnabled=true;
    await redisSubscriber.subscribe(CHAT_CHANNEL, async message=>{
      try{
        const payload=JSON.parse(message||'{}');
        if(payload.gameId) await broadcastChat(payload.gameId);
      }catch(err){
        console.error('Chat subscription error', err.message);
      }
    });
    await redisSubscriber.subscribe(POLL_CHANNEL, async message=>{
      try{
        const payload=JSON.parse(message||'{}');
        if(payload.gameId) await broadcastPoll(payload.gameId);
      }catch(err){
        console.error('Poll subscription error', err.message);
      }
    });
    await hydrateAuthorCountsFromRedis();
    console.log(`[redis] Connected to ${redisUrl}`);
  }catch(err){
    redisEnabled=false;
    redisClient=null;
    redisPublisher=null;
    redisSubscriber=null;
    console.warn(`[redis] Disabled (${err.message}). Falling back to local memory.`);
  }
}

async function hydrateAuthorCountsFromRedis(){
  if(!redisEnabled || !redisClient) return;
  try{
    const stats=await redisClient.hGetAll('chat:authorCounts');
    Object.entries(stats||{}).forEach(([author,count])=>{
      chatAuthorCounts.set(author, Number(count)||0);
    });
  }catch(err){
    console.error('Author count load failed', err.message);
  }
}

async function initTaskQueues(){
  if(!Queue || !Worker){
    recapQueue=null;
    analyticsQueue=null;
    clipQueue=null;
    queuesReady=false;
    console.warn('[queue] Disabled (BullMQ unavailable)');
    return;
  }
  try{
    const connection={ connection:{ url:DEFAULT_REDIS_URL } };
    recapQueue=new Queue(QUEUE_NAMES.recap, connection);
    analyticsQueue=new Queue(QUEUE_NAMES.analytics, connection);
    clipQueue=new Queue(QUEUE_NAMES.clips, connection);
    taskWorkers.push(new Worker(QUEUE_NAMES.recap, processRecapJob, connection));
    taskWorkers.push(new Worker(QUEUE_NAMES.analytics, processAnalyticsJob, connection));
    taskWorkers.push(new Worker(QUEUE_NAMES.clips, processClipIndexJob, connection));
    taskWorkers.forEach(worker=>{
      worker.on('failed',(job,err)=>console.error(`[queue:${worker.name}] job ${job?.id||'?'} failed`, err));
      worker.on('completed',job=>console.log(`[queue:${worker.name}] job ${job.id} completed`));
    });
    queuesReady=true;
    console.log('[queue] Background workers online via BullMQ');
  }catch(err){
    recapQueue=null;
    analyticsQueue=null;
    clipQueue=null;
    queuesReady=false;
    console.warn(`[queue] Disabled (${err.message || 'worker unavailable'})`);
    console.debug(err);
  }
}

async function processRecapJob(job){
  const gameId=job.data?.gameId;
  if(!gameId) throw new Error('gameId required');
  const game=await resolveGameForJob(gameId);
  if(!game) throw new Error(`Game ${gameId} not found`);
  const text=generateRecapText(game);
  if(!text) throw new Error('Recap unavailable for this game');
  const entry=storeRecapEntry(game, text);
  await persistRecaps();
  return entry;
}

async function processAnalyticsJob(){
  const summary=buildAnalyticsSummary();
  await persistAnalyticsSummary(summary);
  return summary;
}

async function processClipIndexJob(job){
  const clipIds=Array.isArray(job.data?.clipIds) ? job.data.clipIds.map(id=>String(id)) : [];
  const source=clipIds.length ? clipStore.filter(entry=>clipIds.includes(String(entry.id))) : clipStore;
  const items=source.map(clip=>({
    id:clip.id,
    title:clip.title,
    author:clip.author,
    sourceType:clip.sourceType,
    createdAt:clip.createdAt,
    tags:Object.keys(clip.meta||{})
  }));
  clipIndexCache={ items, updatedAt:Date.now() };
  await fs.writeFile(CLIP_INDEX_FILE, JSON.stringify(clipIndexCache, null, 2));
  return { count:items.length };
}

async function resolveGameForJob(gameId){
  const match=latestScheduleGames.find(game=>String(game.id)===String(gameId));
  if(match) return match;
  if(dataProvider?.getGame){
    try{
      return await dataProvider.getGame(gameId);
    }catch(err){
      console.error('Provider getGame failed', err.message);
    }
  }
  return null;
}

async function enqueueRecapJob(gameId){
  if(queuesReady && recapQueue){
    const job=await recapQueue.add('generate-recap',{ gameId:String(gameId) },{ removeOnComplete:50, removeOnFail:25 });
    return { status:'queued', jobId:job.id };
  }
  const result=await processRecapJob({ data:{ gameId:String(gameId) }, id:`inline-${Date.now()}` });
  return { status:'completed', jobId:`inline-${Date.now()}`, result };
}

async function enqueueAnalyticsJob(params={}){
  if(queuesReady && analyticsQueue){
    const job=await analyticsQueue.add('rollup', params,{ removeOnComplete:5, removeOnFail:5 });
    return { status:'queued', jobId:job.id };
  }
  const result=await processAnalyticsJob({ data:params, id:`inline-${Date.now()}` });
  return { status:'completed', jobId:`inline-${Date.now()}`, result };
}

async function enqueueClipIndexJob(data={}){
  if(queuesReady && clipQueue){
    const job=await clipQueue.add('reindex', data,{ removeOnComplete:20, removeOnFail:10 });
    return { status:'queued', jobId:job.id };
  }
  const result=await processClipIndexJob({ data, id:`inline-${Date.now()}` });
  return { status:'completed', jobId:`inline-${Date.now()}`, result };
}

async function loadProfileCache(file, target){
  try{
    const raw=await fs.readFile(file,'utf-8');
    const data=JSON.parse(raw);
    Object.entries(data||{}).forEach(([key,value])=>{
      if(value && value.data){
        target.set(key,{ data:value.data, ts:value.ts||0 });
      }
    });
  }catch(err){
    console.error('Profile cache load failed', file, err.message);
  }
}

async function persistProfileCache(file, map){
  const payload={};
  map.forEach((value,key)=>{
    payload[key]={ data:value.data, ts:value.ts };
  });
  await fs.writeFile(file, JSON.stringify(payload,null,2));
}

async function loadVideoCatalog(){
  try{
    const data=await loadJson(VIDEOS_FILE);
    if(Array.isArray(data?.videos)){
      videoCatalog=data.videos;
    }else if(Array.isArray(data)){
      videoCatalog=data;
    }else{
      videoCatalog=[];
    }
  }catch(err){
    console.error('Video catalog load failed', err.message);
    videoCatalog=[];
  }
}

async function loadAnalyticsEvents(){
  try{
    const data=await loadJson(ANALYTICS_FILE) || { events:[] };
    const events=Array.isArray(data?.events)?data.events:(Array.isArray(data)?data:[]);
    events.forEach(evt=>analyticsEvents.push(evt));
  }catch(err){
    console.error('Analytics load failed', err.message);
  }
}

async function loadFanPosts(){
  try{
    const data=await loadJson(FANPOST_FILE) || {};
    Object.entries(data).forEach(([key,list])=>{
      fanPostsStore.set(key, Array.isArray(list)?list.slice(-50):[]);
    });
  }catch(err){
    console.error('Fan posts load failed', err.message);
  }
}

async function persistFanPosts(){
  const payload={};
  fanPostsStore.forEach((posts,key)=>{
    payload[key]=posts.slice(-50);
  });
  await fs.writeFile(FANPOST_FILE, JSON.stringify(payload,null,2));
}

async function loadPredictions(){
  try{
    const data=await loadJson(PREDICTIONS_FILE) || { predictions:[], leagues:[] };
    predictionsLog=Array.isArray(data?.predictions)?data.predictions:[];
    leagueStore=Array.isArray(data?.leagues)?data.leagues:[];
  }catch(err){
    console.error('Predictions load failed', err.message);
    predictionsLog=[];
    leagueStore=[];
  }
}

async function persistPredictions(){
  await fs.writeFile(PREDICTIONS_FILE, JSON.stringify({ predictions:predictionsLog, leagues:leagueStore }, null, 2));
}

async function loadRecaps(){
  try{
    const data=await loadJson(RECAPS_FILE) || {};
    Object.entries(data).forEach(([key,val])=>{
      if(val && typeof val.text==='string'){
        recapStore.set(key, val);
      }
    });
  }catch(err){
    console.error('Recap load failed', err.message);
  }
}

async function persistRecaps(){
  const payload={};
  recapStore.forEach((val,key)=>{ payload[key]=val; });
  await fs.writeFile(RECAPS_FILE, JSON.stringify(payload,null,2));
}

async function loadNotifyStore(){
  try{
    const data=await loadJson(NOTIFY_FILE) || { subscriptions:{} };
    const entries=data.subscriptions || {};
    Object.entries(entries).forEach(([owner, list])=>{
      notifyStore.set(owner, Array.isArray(list)?list.map(normalizeSubscription):[]);
    });
  }catch(err){
    console.error('Notify store load failed', err.message);
  }
}

async function persistNotifyStore(){
  const payload={ subscriptions:{} };
  notifyStore.forEach((list, owner)=>{
    payload.subscriptions[owner]=list.map(sub=>({
      ...sub,
      lastNotified:sub.lastNotified||{}
    }));
  });
  await fs.writeFile(NOTIFY_FILE, JSON.stringify(payload,null,2));
}

async function loadEloStore(){
  try{
    const data=await loadJson(ELO_FILE) || { teams:{}, processedGames:{}, updatedAt:Date.now() };
    eloRatings=new Map();
    Object.entries(data.teams||{}).forEach(([teamId, entry])=>{
      if(entry && typeof entry==='object'){
        entry.rating=Number(entry.rating)||BASE_ELO_RATING;
        entry.wins=Number(entry.wins)||0;
        entry.losses=Number(entry.losses)||0;
        entry.ties=Number(entry.ties)||0;
        entry.games=Number(entry.games)||0;
        entry.history=Array.isArray(entry.history)?entry.history.slice(-HISTORY_LIMIT):[];
        eloRatings.set(teamId, entry);
      }
    });
    processedEloGames=new Set(Object.keys(data.processedGames||{}));
    eloUpdatedAt=data.updatedAt || Date.now();
  }catch(err){
    console.error('Elo store load failed', err.message);
    eloRatings=new Map();
    processedEloGames=new Set();
    eloUpdatedAt=Date.now();
  }
}

async function persistEloStore(){
  const payload={
    teams:Object.fromEntries(eloRatings),
    processedGames:{},
    updatedAt:Date.now()
  };
  processedEloGames.forEach(id=>{
    if(id) payload.processedGames[id]=true;
  });
  await fs.writeFile(ELO_FILE, JSON.stringify(payload,null,2));
  eloUpdatedAt=payload.updatedAt;
}

async function loadHistoryStore(){
  try{
    historyStore=await loadJson(HISTORY_FILE) || {};
  }catch{
    historyStore={};
  }
}

async function persistHistoryStore(){
  await fs.writeFile(HISTORY_FILE, JSON.stringify(historyStore,null,2));
}

function cacheHistorySeason(league, year, payload){
  if(!league || !year || !payload) return;
  const key=league.toLowerCase();
  historyStore[key]=historyStore[key]||{};
  historyStore[key][year]={ ts:Date.now(), data:payload };
  persistHistoryStore().catch(err=>console.error('History persist failed', err.message));
}

async function loadClips(){
  try{
    const data=await loadJson(CLIPS_FILE) || { clips:[] };
    clipStore=Array.isArray(data.clips)?data.clips:[];
  }catch{
    clipStore=[];
  }
}

async function persistClips(){
  await fs.writeFile(CLIPS_FILE, JSON.stringify({ clips:clipStore }, null, 2));
}

async function loadCreatorContent(){
  try{
    const data=await loadJson(CREATOR_CONTENT_FILE) || { creators:[], entries:[] };
    creatorProfiles=Array.isArray(data.creators)?data.creators:[];
    creatorEntries=Array.isArray(data.entries)?data.entries:[];
  }catch{
    creatorProfiles=[];
    creatorEntries=[];
  }
}

async function persistCreatorContent(){
  await fs.writeFile(CREATOR_CONTENT_FILE, JSON.stringify({ creators:creatorProfiles, entries:creatorEntries }, null, 2));
}

async function loadDevicePairs(){
  try{
    const data=await loadJson(DEVICE_PAIR_FILE) || { codes:{} };
    const now=Date.now();
    Object.entries(data.codes||{}).forEach(([code, entry])=>{
      if(entry && entry.expiresAt && entry.expiresAt>now && entry.payload){
        devicePairStore.set(code, entry);
      }
    });
  }catch{
    devicePairStore.clear();
  }
}

async function persistDevicePairs(){
  const now=Date.now();
  const payload={ codes:{} };
  devicePairStore.forEach((entry, code)=>{
    if(entry && entry.expiresAt>now){
      payload.codes[code]=entry;
    }
  });
  await fs.writeFile(DEVICE_PAIR_FILE, JSON.stringify(payload,null,2));
}

function cleanupDevicePairs(){
  const now=Date.now();
  let changed=false;
  devicePairStore.forEach((entry, code)=>{
    if(!entry || entry.expiresAt<=now){
      devicePairStore.delete(code);
      changed=true;
    }
  });
  if(changed){
    persistDevicePairs().catch(err=>console.error('Device pair persist failed', err.message));
  }
}

function generatePairCode(){
  let attempt=0;
  while(attempt<50){
    let code='';
    for(let i=0;i<PAIR_CODE_LENGTH;i++){
      code+=PAIR_CODE_CHARS[Math.floor(Math.random()*PAIR_CODE_CHARS.length)];
    }
    if(!devicePairStore.has(code)) return code;
    attempt++;
  }
  return `${Date.now()}`.slice(-PAIR_CODE_LENGTH);
}

function normalizePairTray(list){
  if(!Array.isArray(list)) return [];
  return list.filter(item=>item && item.url).slice(0,16).map(item=>({
    id:item.id || item.gameId || null,
    title:String(item.title||'').slice(0,120),
    url:item.url,
    provider:item.provider||'admin',
    start:item.start||null,
    league:item.league||''
  }));
}

function trimChatLogPayload(payload){
  if(!payload || typeof payload!=='object') return {};
  const out={};
  Object.entries(payload).forEach(([room,messages])=>{
    if(Array.isArray(messages) && messages.length){
      out[room]=messages.slice(-20);
    }
  });
  return out;
}

function normalizePairPreferences(prefs){
  if(!prefs || typeof prefs!=='object') return null;
  const normalized={
    leagues:Array.isArray(prefs.leagues)&&prefs.leagues.length ? prefs.leagues.map(l=>String(l).toLowerCase()) : DEFAULT_PREFERENCES.leagues,
    provider:(prefs.provider||DEFAULT_PREFERENCES.provider).toLowerCase(),
    discoveryBias:['favorites','mixed','hot'].includes((prefs.discoveryBias||'').toLowerCase()) ? (prefs.discoveryBias||'').toLowerCase() : DEFAULT_PREFERENCES.discoveryBias
  };
  return normalized;
}

function sanitizeDeviceLabel(label){
  if(!label) return null;
  return String(label).trim().slice(0,80) || null;
}

function normalizePairFavorites(list){
  if(!Array.isArray(list)) return [];
  return list.slice(0,30);
}

function buildPairPayload(body={}){
  return {
    tray: normalizePairTray(body.tray),
    preferences: normalizePairPreferences(body.preferences) || DEFAULT_PREFERENCES,
    favorites: normalizePairFavorites(body.favorites),
    chatLog: trimChatLogPayload(body.chatLog),
    device: sanitizeDeviceLabel(body.device),
    user: body.user && typeof body.user==='object'
      ? { id: body.user.id || null, name: String(body.user.name||'').slice(0,80) }
      : null
  };
}

function creatorSlug(name){
  return (name||'').toString().trim().toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'') || `creator-${crypto.randomUUID().slice(0,8)}`;
}

function findCreatorById(id){
  return creatorProfiles.find(c=>c.id===id);
}

function upsertCreatorProfile(payload){
  const now=Date.now();
  let creator=null;
  if(payload.creatorId){
    creator=findCreatorById(payload.creatorId);
  }
  const name=(payload.creatorName||payload.name||'').toString().trim();
  if(!creator && name){
    creator={
      id:payload.creatorId || creatorSlug(name),
      name,
      avatar:(payload.avatar||'').toString().trim(),
      channel:(payload.channel||'').toString().trim(),
      bio:(payload.bio||'').toString().trim(),
      submissions:0,
      followers:payload.followers || Math.floor(Math.random()*5000)+250,
      createdAt:now,
      lastUpload:null
    };
    creatorProfiles.push(creator);
  }else if(creator){
    if(name) creator.name=name;
    if(payload.avatar) creator.avatar=payload.avatar.toString().trim();
    if(payload.channel) creator.channel=payload.channel.toString().trim();
    if(payload.bio) creator.bio=payload.bio.toString().trim();
  }
  return creator;
}

function normalizeCreatorEntry(entry){
  if(!entry || !entry.creatorId || !entry.url) return null;
  return {
    id:entry.id || crypto.randomUUID(),
    creatorId:entry.creatorId,
    creatorName:entry.creatorName,
    title:entry.title,
    description:entry.description||'',
    url:entry.url,
    type:entry.type==='audio'?'audio':'video',
    duration:entry.duration||'',
    tags:Array.isArray(entry.tags)?entry.tags.slice(0,5):[],
    ts:entry.ts || Date.now(),
    thumbnail:entry.thumbnail||''
  };
}

async function loadSubscriptions(){
  try{
    const data=await loadJson(SUBS_FILE) || { subs:{} };
    Object.entries(data.subs||{}).forEach(([owner, record])=>{
      if(record && record.status==='active'){
        subscriptionMap.set(owner, { ...record, owner });
      }
    });
  }catch{
    subscriptionMap.clear();
  }
}

async function persistSubscriptions(){
  const payload={ subs:{} };
  subscriptionMap.forEach((record, owner)=>{
    payload.subs[owner]=record;
  });
  await fs.writeFile(SUBS_FILE, JSON.stringify(payload,null,2));
}

function isPremium(ownerKey){
  if(!ownerKey) return false;
  const record=subscriptionMap.get(ownerKey);
  return record?.status==='active';
}

function getSubscriptionRecord(ownerKey){
  if(!ownerKey) return null;
  return subscriptionMap.get(ownerKey) || null;
}


function scheduleAnalyticsFlush(){
  if(analyticsFlushTimer) return;
  analyticsFlushTimer=setTimeout(async ()=>{
    analyticsFlushTimer=null;
    await persistAnalytics();
  }, 2000);
}

async function persistAnalytics(){
  const trimmed=analyticsEvents.slice(-ANALYTICS_MAX_EVENTS);
  await fs.writeFile(ANALYTICS_FILE, JSON.stringify({ events:trimmed }, null, 2));
}

function logAnalyticsEvent(type, payload={}){
  const event={ id:crypto.randomUUID(), type, ts:Date.now(), ...payload };
  analyticsEvents.push(event);
  if(analyticsEvents.length>ANALYTICS_MAX_EVENTS){
    analyticsEvents.splice(0, analyticsEvents.length-ANALYTICS_MAX_EVENTS);
  }
  scheduleAnalyticsFlush();
  return event;
}

function hashPassword(password, salt){
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
}
function createToken(){
  return crypto.randomBytes(24).toString('hex');
}
function safeUser(user){
  return { id:user.id, name:user.name, email:user.email };
}

const tokens = new Map(); // token -> userId

function resolveUserFromRequest(req){
  if(req.userId) return req.userId;
  const header=req.headers.authorization||'';
  if(header){
    const token=header.replace('Bearer','').trim();
    if(token && tokens.has(token)) return tokens.get(token);
  }
  return null;
}

function resolveOwnerKey(req){
  const userId=resolveUserFromRequest(req);
  if(userId) return userId;
  const guestId=(req.headers['x-guest-id']||req.query.guestId||'').toString().trim();
  if(guestId) return `guest:${guestId}`;
  return null;
}

function normalizeFavoriteList(list=[]){
  return list.filter(f=>f && f.id).map(f=>{
    const type = f.type==='player' ? 'player' : 'team';
    return {
      id:String(f.id),
      type,
      name:f.name || '',
      league:f.league || '',
      logo:f.logo || '',
      headshot:f.headshot || '',
      meta:f.meta || f.position || '',
      team:f.team || null
    };
  });
}

function isValidHttpUrl(value){
  if(!value || typeof value!=='string') return false;
  try{
    const url=new URL(value);
    return url.protocol==='http:' || url.protocol==='https:';
  }catch{
    return false;
  }
}

function normalizeProfileDetails(details={}){
  const avatar=isValidHttpUrl(details.avatar) ? details.avatar.trim() : '';
  const bio=String(details.bio||'').trim().slice(0,280);
  const favoriteTeams=Array.isArray(details.favoriteTeams)
    ? details.favoriteTeams.map(team=>String(team||'').trim()).filter(Boolean).slice(0,6)
    : [];
  const badges=Array.isArray(details.badges)
    ? details.badges.map((badge,idx)=>({
        id:String(badge?.id || `badge-${idx}`),
        label:String(badge?.label || '').trim().slice(0,40),
        detail:badge?.detail ? String(badge.detail).trim().slice(0,140) : ''
      })).filter(badge=>badge.label).slice(0,6)
    : [];
  return { avatar, bio, favoriteTeams, badges };
}

function defaultProfileDetails(){
  return { avatar:'', bio:'', favoriteTeams:[], badges:[] };
}

function normalizeProfileRecord(record={}, user={}){
  const base=record && typeof record==='object' ? record : {};
  return {
    favorites: normalizeFavoriteList(base.favorites||[]),
    watchlist: Array.isArray(base.watchlist) ? base.watchlist : [],
    preferences: { ...DEFAULT_PREFERENCES, ...(base.preferences||{}) },
    details: normalizeProfileDetails(base.details||{})
  };
}

function ensureProfileRecord(data, user){
  if(!data.profiles) data.profiles={};
  const normalized=normalizeProfileRecord(data.profiles[user.id], user);
  data.profiles[user.id]=normalized;
  return normalized;
}

function deriveFavoriteLabels(list=[], limit=6){
  return list.filter(entry=>entry.type==='team' && entry.name)
    .map(entry=>entry.name)
    .filter(Boolean)
    .slice(0,limit);
}

function defaultAvatarFor(name){
  const seed=encodeURIComponent(name || 'fan');
  return `https://api.dicebear.com/7.x/initials/svg?seed=${seed}`;
}

function countPredictionWins(userId){
  if(!userId) return 0;
  return predictionsLog.filter(entry=>entry.userKey===userId && entry.correct).length;
}

function countChatMessagesByAuthor(author){
  if(!author) return 0;
  if(chatAuthorCounts.has(author)){
    return chatAuthorCounts.get(author)||0;
  }
  if(!redisEnabled){
    let total=0;
    chatStore.forEach(messages=>{
      (messages||[]).forEach(msg=>{
        if(msg.author===author) total+=1;
      });
    });
    chatAuthorCounts.set(author, total);
    return total;
  }
  return 0;
}

function dedupeBadges(badges){
  const seen=new Set();
  const output=[];
  badges.forEach(badge=>{
    if(!badge || !badge.label) return;
    const key=(badge.id || badge.label || `badge-${output.length}`).toLowerCase();
    if(seen.has(key)) return;
    seen.add(key);
    output.push({
      id:key,
      label:badge.label,
      detail:badge.detail || ''
    });
  });
  return output;
}

function buildBadgeDeck(user, profile, details){
  const baseBadges=Array.isArray(details.badges)?details.badges:[];
  const badges=[...baseBadges];
  const favCount=profile.favorites?.length||0;
  if(favCount>=5){
    badges.push({ id:'superfan', label:'Superfan', detail:`Following ${favCount} teams` });
  }
  const winCount=countPredictionWins(user.id);
  if(winCount>=3){
    badges.push({ id:'prediction-streak', label:'Prediction Streak', detail:`${winCount} winning picks` });
  }
  const chatCount=countChatMessagesByAuthor(user.name);
  if(chatCount>=10){
    badges.push({ id:'top-chatter', label:'Top Chatter', detail:`${chatCount} Fan Zone messages` });
  }
  return dedupeBadges(badges);
}

function buildPublicProfile(user, profile){
  const details=profile.details || defaultProfileDetails();
  const favoriteTeams=details.favoriteTeams.length ? details.favoriteTeams : deriveFavoriteLabels(profile.favorites);
  return {
    id:user.id,
    name:user.name,
    avatar:details.avatar || defaultAvatarFor(user.name),
    bio:details.bio,
    favoriteTeams,
    badges:buildBadgeDeck(user, profile, details),
    joined:user.createdAt
  };
}

function socialTargetKey({ teamId, gameId, query }={}){
  if(gameId) return `game:${gameId}`;
  if(teamId) return `team:${teamId}`;
  if(query) return `q:${String(query).toLowerCase()}`;
  return 'global';
}

function getFanPosts(key){
  if(!key) return [];
  return (fanPostsStore.get(key)||[]).slice(-30);
}

function addFanPost(key, entry){
  if(!key || !entry) return;
  const list=fanPostsStore.get(key)||[];
  list.push(entry);
  fanPostsStore.set(key, list.slice(-50));
}

function predictorKey(req){
  const userId=resolveUserFromRequest(req);
  if(userId) return userId;
  const guestId=(req.headers['x-guest-id']||req.body?.guestId||`guest:${req.ip||'anon'}`).toString();
  return guestId.startsWith('guest:')?guestId:`guest:${guestId}`;
}

function canonicalPrediction(entry){
  if(!entry) return null;
  const pick=entry.pick==='home'?'home':(entry.pick==='away'?'away':'draw');
  const margin=Number(entry.margin)||0;
  const overUnder=entry.overUnder==='over'?'over':(entry.overUnder==='under'?'under':null);
  const firstScore=entry.firstScore ? String(entry.firstScore).slice(0,24) : null;
  return { pick, margin, overUnder, firstScore };
}

function evaluatePredictionEntry(entry, result){
  if(!entry || entry.resultEvaluated || !result) return entry;
  let points=0;
  let correct=false;
  if(entry.prediction.pick && entry.prediction.pick===result.winner){
    points+=2;
    correct=true;
  }
  if(entry.prediction.margin && result.margin!==null){
    if(Math.abs(result.margin-entry.prediction.margin)<=5) points+=1;
  }
  if(entry.prediction.overUnder && result.overUnder){
    if(entry.prediction.overUnder===result.overUnder) points+=1;
  }
  entry.points=points;
  entry.correct=correct && points>0;
  entry.resultEvaluated=true;
  entry.resultSummary=result;
  return entry;
}

function evaluatePredictionsForGame(game){
  if(!game || game.status?.state?.toLowerCase()!=='post') return;
  const homeScore=parseScoreValue(game.home?.score);
  const awayScore=parseScoreValue(game.away?.score);
  const winner=homeScore===awayScore?'draw':(homeScore>awayScore?'home':'away');
  const margin=Math.abs(homeScore-awayScore);
  const total=homeScore+awayScore;
  const overUnder=game.odds?.overUnder ? (total > Number(game.odds.overUnder) ? 'over' : 'under') : null;
  const result={ winner, margin, overUnder };
  predictionsLog.forEach(entry=>{
    if(entry.gameId===game.id && !entry.resultEvaluated){
      evaluatePredictionEntry(entry, result);
    }
  });
}

function computeLeaderboard(filterFn=null, nameMap=null){
  const board=new Map();
  predictionsLog.forEach(entry=>{
    const key=entry.userKey || entry.author || 'fan';
    if(filterFn && !filterFn(entry)) return;
    if(!board.has(key)){
      board.set(key,{ userKey:key, name:entry.author||'Fan', points:0, correct:0, attempts:0 });
    }
    const row=board.get(key);
    if(nameMap && nameMap.has(key)){
      row.name=nameMap.get(key);
    }else if(entry.author){
      row.name=entry.author;
    }
    row.attempts+=1;
    if(entry.resultEvaluated){
      row.points+=entry.points||0;
      if(entry.correct) row.correct+=1;
    }
  });
  return [...board.values()].sort((a,b)=>b.points-a.points || b.correct-a.correct).slice(0,20);
}

const LEADERBOARD_WINDOWS=['week','month','season'];

function leaderboardRange(windowKey){
  const now=new Date();
  const end=Date.now();
  if(windowKey==='week'){
    const start=new Date(now);
    const day=start.getUTCDay();
    const diff=(day+6)%7; // Monday start
    start.setUTCDate(start.getUTCDate()-diff);
    start.setUTCHours(0,0,0,0);
    return { start:start.getTime(), end };
  }
  if(windowKey==='month'){
    const start=new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
    return { start:start.getTime(), end };
  }
  return { start:null, end };
}

function generateInviteCode(){
  let code='';
  do{
    code=crypto.randomBytes(3).toString('hex').toUpperCase();
  }while(leagueStore.some(league=>league.code===code));
  return code;
}

function findLeagueById(id){
  if(!id) return null;
  return leagueStore.find(league=>league.id===id);
}

function findLeagueByCode(code){
  if(!code) return null;
  const normalized=code.trim().toUpperCase();
  return leagueStore.find(league=>(league.code||'').toUpperCase()===normalized);
}

function getUserLeagues(userId){
  if(!userId) return [];
  return leagueStore.filter(league=>(league.members||[]).some(member=>member.userId===userId));
}

function ensureLeagueMember(league, user){
  if(!league || !user) return false;
  league.members=league.members||[];
  if(league.members.some(member=>member.userId===user.id)) return false;
  league.members.push({ userId:user.id, name:user.name, joinedAt:Date.now() });
  return true;
}

function publicLeagueData(league, userId){
  if(!league) return null;
  const isOwner=league.ownerId===userId;
  return {
    id:league.id,
    name:league.name,
    memberCount:(league.members||[]).length,
    members:(league.members||[]).map(member=>({ id:member.userId, name:member.name, joinedAt:member.joinedAt })),
    isOwner,
    inviteCode:isOwner ? league.code : undefined,
    createdAt:league.createdAt
  };
}

function recapKey(gameId){
  return String(gameId);
}

function getRecapEntry(gameId){
  return recapStore.get(recapKey(gameId));
}

function storeRecapEntry(game, text){
  if(!text) return null;
  const entry={
    id:recapKey(game.id),
    gameId:String(game.id),
    league:game.league,
    date:toEasternISO(game.start || new Date()),
    away:game.away?.name,
    home:game.home?.name,
    finalScore:`${game.away?.score ?? '0'}-${game.home?.score ?? '0'}`,
    text,
    createdAt:Date.now()
  };
  recapStore.set(entry.id, entry);
  return entry;
}

function extractTopPerformer(game, side){
  const stats=game.insights?.playerStats?.[side];
  if(!stats || !stats.length) return null;
  const leader=stats[0];
  if(!leader?.name || !leader?.value) return null;
  return `${leader.name} (${leader.value})`;
}

function generateRecapText(game){
  const homeScore=parseScoreValue(game.home?.score);
  const awayScore=parseScoreValue(game.away?.score);
  if(!Number.isFinite(homeScore) || !Number.isFinite(awayScore)) return null;
  const state=(game.status?.state||'').toLowerCase();
  if(state!=='post') return null;
  const home=game.home || {};
  const away=game.away || {};
  let winner=home;
  let loser=away;
  let winnerScore=homeScore;
  let loserScore=awayScore;
  if(awayScore>homeScore){
    winner=away;
    loser=home;
    winnerScore=awayScore;
    loserScore=homeScore;
  }
  const margin=Math.abs(homeScore-awayScore);
  const sentences=[];
  sentences.push(`${winner.name||'The home side'} ${margin<=3?'slipped past':'defeated'} ${loser.name||'their opponent'} ${winnerScore}-${loserScore}.`);
  if(margin<=3){
    sentences.push('It was tight down the stretch with every possession mattering in the closing minutes.');
  }else if(margin>=15){
    sentences.push(`${winner.name||'The winners'} controlled the matchup from the opening tip and never looked back.`);
  }else{
    sentences.push(`${winner.name||'The winners'} built their cushion during the middle frames and held steady late.`);
  }
  const topWinner = winner===home ? extractTopPerformer(game,'home') : extractTopPerformer(game,'away');
  const topLoser = winner===home ? extractTopPerformer(game,'away') : extractTopPerformer(game,'home');
  if(topWinner){
    sentences.push(`${topWinner} powered the effort for ${winner.short||winner.name||'the winners'}.`);
  }
  if(topLoser){
    sentences.push(`${topLoser} led the resistance for ${loser.short||loser.name||'the opposition'}.`);
  }
  sentences.push(`${game.league||'The league'} slate now turns toward the next tilt after this result on ${game.dateText||'the night'} went final.`);
  return sentences.filter(Boolean).join(' ');
}

function buildHighlightMoments(game){
  const homeScore=parseScoreValue(game.home?.score);
  const awayScore=parseScoreValue(game.away?.score);
  const state=(game.status?.state||'').toLowerCase();
  const moments=[];
  const winner=homeScore>=awayScore?game.home:game.away;
  const loser=winner===game.home?game.away:game.home;
  const winnerScore=winner===game.home?homeScore:awayScore;
  const loserScore=winner===game.home?awayScore:homeScore;
  if(state==='in' && game.status?.detail){
    moments.push({
      type:'live',
      title:'Live situation',
      detail:game.status.detail
    });
  }
  const momentumDetail=game.insights?.momentum?.detail;
  if(momentumDetail){
    moments.push({
      type:'momentum',
      title:'Momentum swing',
      detail:momentumDetail
    });
  }
  const topWinner = winner===game.home ? extractTopPerformer(game,'home') : extractTopPerformer(game,'away');
  if(topWinner){
    moments.push({
      type:'performance',
      title:`${winner?.short || winner?.name || 'Top performer'} shines`,
      detail:topWinner
    });
  }
  if(game.odds?.overUnder){
    const total=homeScore+awayScore;
    const pick=total>Number(game.odds.overUnder)?'over':'under';
    moments.push({
      type:'totals',
      title:'Totals decided',
      detail:`Combined ${total} pts (${pick}) vs O/U ${game.odds.overUnder}`
    });
  }
  if(state==='post'){
    moments.unshift({
      type:'final',
      title:'Final horn',
      detail:`${winner?.name||'Winner'} closed out ${loser?.name||'their opponent'} ${winnerScore}-${loserScore}.`
    });
  }
  return moments.slice(0,5);
}

function parseRecord(record){
  if(!record) return { wins:0, losses:0, draws:0 };
  const match=record.match(/(\d+)\s*-\s*(\d+)(?:\s*-\s*(\d+))?/);
  if(!match) return { wins:0, losses:0, draws:0 };
  return { wins:Number(match[1])||0, losses:Number(match[2])||0, draws:Number(match[3])||0 };
}

function summarizeTeamForm(team){
  const rec=parseRecord(team.record);
  const total=rec.wins+rec.losses;
  if(!total) return 'Form data unavailable';
  const pct=((rec.wins)/(total||1)*100).toFixed(1);
  return `${rec.wins}-${rec.losses}${rec.draws?`-${rec.draws}`:''} (${pct}% win rate)`;
}

function buildTeamComparison(team, context=null){
  const recordSummary=summarizeTeamForm(team);
  return {
    id:team.id,
    name:team.name,
    record:team.record || '—',
    summary:team.summary || '',
    venue:team.venue || '',
    nextEvent:team.nextEvent || '',
    form:recordSummary,
    rosterCount:team.roster?.length || 0,
    links:team.links || [],
    league:context?.game?.league || team.league || '',
    stats:team.stats || [],
    rankings:team.rankings || [],
    odds:context?.game?.odds || null
  };
}

function buildPlayerComparison(player){
  return {
    id:player.id,
    name:player.fullName,
    position:player.position || '',
    team:player.team || null,
    bio:player.bio || '',
    stats:player.stats || [],
    age:player.age || null,
    height:player.height || '',
    weight:player.weight || ''
  };
}

function buildComparisonSummary(type, a, b){
  if(type==='player'){
    return `${a.name} (${a.position||'—'}) vs ${b.name} (${b.position||'—'}) — both bringing unique skill-sets. ${a.name} represents ${a.team?.name||'their team'}, while ${b.name} lines up for ${b.team?.name||'their side'}.`;
  }
  const formTextA=a.form || 'Form unavailable';
  const formTextB=b.form || 'Form unavailable';
  return `${a.name} (${formTextA}) squares off with ${b.name} (${formTextB}). ${a.name} plays at ${a.venue||'home'}, while ${b.name} prepares out of ${b.venue||'their venue'}.`;
}

function findRecentTeamContext(teamId){
  const target=String(teamId);
  if(!target) return null;
  for(const game of latestScheduleGames){
    if(String(game.home?.id)===target){
      return { game, side:'home' };
    }
    if(String(game.away?.id)===target){
      return { game, side:'away' };
    }
  }
  return null;
}

function gatherTeamInjuries(profile, context){
  const list=[];
  const seen=new Set();
  const targetId=String(profile?.id||'');
  if(context?.game?.injuries?.length){
    context.game.injuries.forEach(note=>{
      const matches=note.teamId ? String(note.teamId)===targetId : true;
      if(!matches) return;
      const sig=`ctx:${note.athlete||''}:${note.detail||''}:${note.status||''}`;
      if(seen.has(sig)) return;
      seen.add(sig);
      list.push(note);
    });
  }
  (profile?.injuries||[]).forEach(note=>{
    const sig=`profile:${note.athlete||''}:${note.detail||''}:${note.status||''}`;
    if(seen.has(sig)) return;
    seen.add(sig);
    list.push(note);
  });
  return list;
}

function buildCompareOdds(contextA, contextB){
  const ctx=contextA?.game?.odds ? contextA : (contextB?.game?.odds ? contextB : null);
  if(!ctx?.game?.odds) return null;
  return {
    ...ctx.game.odds,
    gameId:ctx.game.id
  };
}

function normalizeSubscription(entry){
  if(!entry || !entry.targetId) return null;
  const type=entry.targetType==='team'?'team':'game';
  return {
    id:entry.id || crypto.randomUUID(),
    targetType:type,
    targetId:String(entry.targetId),
    createdAt:entry.createdAt || Date.now(),
    lastNotified:{
      start:Boolean(entry.lastNotified?.start),
      final:Boolean(entry.lastNotified?.final),
      scoreSig:entry.lastNotified?.scoreSig || null
    }
  };
}

function todayStr(){
  const formatter = new Intl.DateTimeFormat('en-US', { timeZone:'America/New_York', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', hour12:false });
  const parts = formatter.formatToParts(new Date()).reduce((acc, part)=>{ if(part.type!=='literal') acc[part.type]=part.value; return acc; },{});
  const year=parseInt(parts.year,10);
  const month=parseInt(parts.month,10);
  const day=parseInt(parts.day,10);
  const hour=parseInt(parts.hour,10);
  const base=new Date(Date.UTC(year, month-1, day));
  if(hour < SCOREBOARD_ROLLOVER_HOUR){ base.setUTCDate(base.getUTCDate()-1); }
  const finalYear=base.getUTCFullYear();
  const finalMonth=String(base.getUTCMonth()+1).padStart(2,'0');
  const finalDay=String(base.getUTCDate()).padStart(2,'0');
  return `${finalYear}-${finalMonth}-${finalDay}`;
}
function toYYYYMMDD(iso){ return iso.replace(/-/g,''); }

async function fetchJSON(url){
  const res = await fetch(url);
  if(!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchEspnLeague(league, iso){
  try{
    if(league==='ufc'){
      const data=await fetchJSON(SPORT_ENDPOINTS.ufc());
      const events=(data?.sports?.[0]?.leagues||[]).flatMap(l=>l?.events||[]);
      return events.map(normalizeUFCEvent).filter(evt=>evt.start && toEasternISO(evt.start)===iso);
    }
    const endpoint = SPORT_ENDPOINTS[league];
    if(!endpoint) return [];
    const data=await fetchJSON(endpoint(toYYYYMMDD(iso)));
    return (data?.events||[]).map(evt=>normalizeEvent(evt, league));
  }catch(err){
    console.error(`Failed to fetch ${league}:`, err.message);
    return [];
  }
}

function normalizeEvent(evt, league){
  const comp=evt?.competitions?.[0]||{};
  const competitors=comp.competitors||[];
  const homeComp=competitors.find(c=>c.homeAway==='home')||competitors[0];
  const awayComp=competitors.find(c=>c.homeAway==='away')||competitors[1];
  const status=comp.status?.type||evt.status?.type||{};
  const clock=comp.status?.displayClock||evt.status?.displayClock||'';
  const startIso=evt.date||comp.date;
  const start=startIso?new Date(startIso):null;
  const dateText=start?start.toLocaleDateString(undefined,{weekday:'short',month:'short',day:'numeric'}):'';
  const timeText=start?start.toLocaleTimeString([], { hour:'numeric', minute:'2-digit' }):'';
  const broadcast=comp.broadcasts?.map(b=>b.names?.join(', ')||b.market).join(' • ');
  const oddsRaw = comp.odds?.[0] || evt.odds?.[0];
  const odds = oddsRaw ? {
    provider: oddsRaw.provider?.name || oddsRaw.provider?.shortName || oddsRaw.provider || 'Consensus',
    details: oddsRaw.details || '',
    spread: oddsRaw.spread,
    overUnder: oddsRaw.overUnder || oddsRaw.total,
    awayMoney: oddsRaw.awayTeamOdds?.moneyLine ?? oddsRaw.awayTeamOdds?.moneyline ?? null,
    homeMoney: oddsRaw.homeTeamOdds?.moneyLine ?? oddsRaw.homeTeamOdds?.moneyline ?? null
  } : null;
  const injuries = (competitors||[]).flatMap(team=>{
    const teamMeta=team.team||{};
    const teamId=teamMeta.id || teamMeta.uid || '';
    return (team.injuries||[]).map(note=>({
      team:teamMeta.displayName||teamMeta.shortDisplayName||teamMeta.abbreviation||'',
      teamId:teamId?String(teamId):'',
      athlete:note.athlete?.displayName||note.athlete?.shortName||'',
      athleteId:note.athlete?.id || note.athlete?.athleteId || '',
      status:note.status?.description || note.status || note.type || '',
      detail:note.detail || note.shortDetail || note.description || ''
    }));
  });
  const mapTeam = teamComp => {
    if(!teamComp) return { id:`${evt.id}-${Math.random()}`, name:'TBD', short:'TBD', score:'—', record:'', logo:'' };
    const team=teamComp.team||{};
    const record=(teamComp.records||[]).find(r=>r.type==='total'||r.name==='overall');
    return {
      id:team.id||teamComp.id,
      name:team.displayName||team.shortDisplayName||team.name||'TBD',
      short:team.abbreviation||team.shortDisplayName||team.name||'—',
      score:teamComp.score ?? '—',
      record:record?.summary || '',
      logo:team.logo||team.logos?.[0]?.href||''
    };
  };
  const home=mapTeam(homeComp);
  const away=mapTeam(awayComp);
  const insights=buildInsightsPayload(comp, homeComp, awayComp, home, away, league);
  return {
    id:evt.id,
    league:league.toUpperCase(),
    home,away,
    status:{
      detail:status.detail||status.description||'',
      shortDetail:status.shortDetail||status.detail||'',
      state:status.state||'pre',
      clock,
      period: typeof status.period==='number' ? status.period : comp.status?.period ?? null
    },
    start,dateText,timeText,
    venue:comp.venue?.fullName||'',
    headline:comp.notes?.[0]?.headline || evt.headlines?.[0]?.shortLinkText || '',
    broadcast,
    links:extractLinks(evt.links||[]),
    odds,
    injuries,
    insights,
    insightsAvailable:Boolean(insights && Object.keys(insights).length)
  };
}

function statValue(stats, keys){
  if(!Array.isArray(stats)) return null;
  const targets=Array.isArray(keys)?keys:[keys];
  const entry=stats.find(stat=>targets.includes(stat.name)||targets.includes(stat.abbreviation));
  if(!entry) return null;
  if(entry.value!=null) return entry.value;
  if(entry.displayValue!=null) return entry.displayValue;
  return null;
}

function buildHistoryStandingsGroups(payload){
  const groups=[];
  const children=payload?.children || payload?.standings?.children || [];
  const collectEntries=(container)=>{
    const entries=container?.standings?.entries || container?.entries || [];
    return entries.map(entry=>{
      const wins=statValue(entry.stats,['wins','gamesWon']);
      const losses=statValue(entry.stats,['losses','gamesLost']);
      const ties=statValue(entry.stats,['ties','gamesTied']);
      const record=(wins!=null && losses!=null)
        ? `${wins}-${losses}${ties!=null?`-${ties}`:''}`
        : (statValue(entry.stats,['record']) || entry.statsSummary || '');
      return {
        team:entry.team?.displayName || entry.team?.name || entry.team?.shortDisplayName,
        logo:entry.team?.logo || entry.team?.logos?.[0]?.href || '',
        wins,
        losses,
        winPct:statValue(entry.stats,['winPercent','percentage']),
        record,
        seed:statValue(entry.stats,['playoffSeed','seed'])
      };
    }).filter(row=>row.team);
  };
  if(children.length){
    children.forEach(child=>{
      const rows=collectEntries(child);
      if(rows.length){
        groups.push({
          name:child.name || child.displayName || child.abbreviation || 'Division',
          rows
        });
      }
    });
  }else if(payload?.standings?.entries){
    const rows=collectEntries(payload.standings);
    if(rows.length){
      groups.push({ name:'Standings', rows });
    }
  }
  return groups;
}

function extractPlayoffResults(payload){
  const events=payload?.events || [];
  return events.map(evt=>{
    const comp=evt.competitions?.[0]||{};
    const competitors=comp.competitors||[];
    const home=competitors.find(c=>c.homeAway==='home')||competitors[0];
    const away=competitors.find(c=>c.homeAway==='away')||competitors[1];
    const matchup=`${away?.team?.shortDisplayName || away?.team?.displayName || 'Away'} @ ${home?.team?.shortDisplayName || home?.team?.displayName || 'Home'}`;
    return {
      id:evt.id,
      name:evt.name,
      shortName:evt.shortName,
      round:comp.series?.summary || comp.notes?.[0]?.headline || evt.season?.type?.name || evt.name,
      status:comp.status?.type?.description || evt.status?.type?.description || '',
      matchup,
      home:{
        name:home?.team?.displayName || home?.team?.shortDisplayName,
        score:home?.score
      },
      away:{
        name:away?.team?.displayName || away?.team?.shortDisplayName,
        score:away?.score
      }
    };
  }).filter(item=>item.home?.name || item.away?.name);
}

function historyScoreboardUrl(config, year){
  if(!config?.scoreboardSlug || !config?.playoffDate) return null;
  const effYear=Number(year)+(Number(config.playoffOffset)||0);
  const dateSegment=String(config.playoffDate).padStart(4,'0');
  return `https://site.api.espn.com/apis/site/v2/sports/${config.scoreboardSlug}/scoreboard?dates=${effYear}${dateSegment}`;
}

async function fetchHistorySeason(league, year){
  const key=league.toLowerCase();
  const config=HISTORY_CONFIG[key];
  if(!config) throw new Error('League not supported');
  const season=Number(year);
  if(!Number.isFinite(season)) throw new Error('Invalid season');
  const standingsUrl=`${config.standings}?season=${season}`;
  const standingsPayload=await fetchJSON(standingsUrl);
  const standings=buildHistoryStandingsGroups(standingsPayload);
  let playoffs=[];
  const scoreboardUrl=historyScoreboardUrl(config, season);
  if(scoreboardUrl){
    try{
      const playoffsPayload=await fetchJSON(scoreboardUrl);
      playoffs=extractPlayoffResults(playoffsPayload);
    }catch(err){
      console.error('History playoffs fetch failed', err.message);
    }
  }
  return {
    league:key.toUpperCase(),
    year:season,
    standings,
    playoffs,
    fetchedAt:Date.now()
  };
}

function normalizeUFCEvent(evt){
  const dt=evt.date?new Date(evt.date):null;
  const match=(evt.name||'').match(/(.+?)\s+vs\.?\s+(.+)/i);
  const away=match?match[1].trim():'Fighter A';
  const home=match?match[2].trim():'Fighter B';
  return {
    id:evt.id||evt.uid,
    league:'UFC',
    home:{ id:`${evt.id}-home`, name:home, short:home, score:'—', record:'', logo:'' },
    away:{ id:`${evt.id}-away`, name:away, short:away, score:'—', record:'', logo:'' },
    status:{ detail:evt.status||'Fight Night', shortDetail:evt.status||'Upcoming', state:'pre', clock:'' },
    start:dt,
    dateText:dt?dt.toLocaleDateString(undefined,{weekday:'short',month:'short',day:'numeric'}):'',
    timeText:dt?dt.toLocaleTimeString([], { hour:'numeric', minute:'2-digit' }):'',
    venue:evt.venue?.fullName||'',
    headline:evt.note||'',
    broadcast:'',
    links:{},
    odds:null,
    injuries:[]
  };
}

function extractLinks(links){
  const out={};
  links.forEach(l=>{
    const rel=(l.rel||[]).join('');
    if(rel.includes('summary')) out.gamecast=l.href;
    if(rel.includes('boxscore')) out.boxscore=l.href;
    if(rel.includes('pbp')) out.playbyplay=l.href;
    if(rel.includes('recap')) out.recap=l.href;
  });
  return out;
}

function toEasternISO(date){
  if(!date) return '';
  const formatter=new Intl.DateTimeFormat('en-US',{ timeZone:'America/New_York', year:'numeric', month:'2-digit', day:'2-digit' });
  const parts=formatter.formatToParts(date).reduce((acc, part)=>{ if(part.type!=='literal') acc[part.type]=part.value; return acc; },{});
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function buildInsightsPayload(comp, homeComp, awayComp, homeTeam, awayTeam, leagueCode){
  const payload={};
  const teamStats=collectTeamStats(comp);
  const playerStats=collectPlayerStats(homeComp, awayComp);
  const momentum=computeMomentumSnapshot(comp, homeTeam, awayTeam);
  const diamond=buildBaseballDiamond(comp, leagueCode);
  if(teamStats) payload.teamStats=teamStats;
  if(playerStats) payload.playerStats=playerStats;
  if(momentum) payload.momentum=momentum;
  if(diamond) payload.diamond=diamond;
  return Object.keys(payload).length ? payload : null;
}

function collectTeamStats(comp){
  const stats=comp?.statistics;
  if(!Array.isArray(stats) || !stats.length) return null;
  const mapped={ home:[], away:[] };
  stats.forEach(stat=>{
    const label=stat.displayName || stat.shortDisplayName || stat.name || stat.description;
    const value=stat.displayValue || stat.value || '';
    if(!label || value===undefined) return;
    const key=stat.homeAway==='home'?'home':(stat.homeAway==='away'?'away':null);
    if(!key) return;
    mapped[key].push({ label, value });
  });
  if(!mapped.home.length && !mapped.away.length) return null;
  return mapped;
}

function flattenLeaderGroup(group){
  if(!group) return [];
  return (group.leaders||[]).map(entry=>({
    name:entry.athlete?.displayName || entry.displayName || 'Player',
    stat:group.displayName || group.category || '',
    value:entry.displayValue || entry.value || '',
    teamId:entry.teamId || entry.athlete?.teamId || null,
    headshot:entry.athlete?.headshot?.href || ''
  }));
}

function collectPlayerStats(homeComp, awayComp){
  const homeLeaders = (homeComp?.leaders||[]).flatMap(flattenLeaderGroup);
  const awayLeaders = (awayComp?.leaders||[]).flatMap(flattenLeaderGroup);
  if(!homeLeaders.length && !awayLeaders.length) return null;
  return {
    home:homeLeaders.slice(0,8),
    away:awayLeaders.slice(0,8)
  };
}

function computeMomentumSnapshot(comp, homeTeam, awayTeam){
  const situation=comp?.situation;
  const lastPlay=situation?.lastPlay;
  const homeScore=parseScoreValue(homeTeam?.score);
  const awayScore=parseScoreValue(awayTeam?.score);
  const leader=homeScore===awayScore ? null : (homeScore>awayScore ? homeTeam?.short : awayTeam?.short);
  const diff=Math.abs(homeScore-awayScore);
  const detail=lastPlay?.text || (leader ? `${leader} on top by ${diff}` : null);
  if(!leader && !detail) return null;
  const trend=lastPlay?.team?.abbreviation || leader || null;
  const momentumValue=Math.max(0, Math.min(1, leader===homeTeam?.short ? 0.65 : leader===awayTeam?.short ? 0.35 : 0.5));
  return {
    leader,
    detail,
    bar:momentumValue,
    lastEvent:lastPlay?.text || null,
    possession:situation?.possession || null,
    arrow:trend
  };
}

function clampStatValue(val, min, max){
  if(val==null) return null;
  const num=typeof val==='number' ? val : parseInt(val,10);
  if(Number.isNaN(num)) return null;
  if(typeof min==='number' && num<min) return min;
  if(typeof max==='number' && num>max) return max;
  return num;
}

function parsePitchTotal(raw){
  if(raw==null) return null;
  if(typeof raw==='number') return raw;
  if(typeof raw==='object'){
    if(typeof raw.pitches==='number') return raw.pitches;
    if(typeof raw.pitchCount==='number') return raw.pitchCount;
    if(typeof raw.count==='number') return raw.count;
  }
  const text=String(raw);
  const match=text.match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

function parseBaseOccupancy(situation){
  const runners=Array.isArray(situation?.baseRunners) ? situation.baseRunners : [];
  const checkRunner=target=>{
    return runners.some(runner=>{
      const candidates=[runner.endingBase, runner.endBase, runner.base, runner.currentBase, runner.startingBase];
      return candidates.some(value=>Number(value)===target);
    });
  };
  const summary=[
    situation?.onBase?.long,
    situation?.onBase?.text,
    situation?.onBase?.displayValue,
    situation?.menOnBase,
    situation?.baseState
  ].filter(Boolean).join(' ').toLowerCase();
  const hasText=pattern=>pattern.test(summary);
  const basesLoaded=hasText(/bases?\s+loaded/);
  return {
    first: basesLoaded || checkRunner(1) || hasText(/first|1st/),
    second: basesLoaded || checkRunner(2) || hasText(/second|2nd/),
    third: basesLoaded || checkRunner(3) || hasText(/third|3rd/)
  };
}

function extractHalfInning(comp){
  const detail=comp?.status?.type?.detail || comp?.status?.type?.shortDetail || '';
  if(/top/i.test(detail)) return 'Top';
  if(/bottom|bot/i.test(detail)) return 'Bottom';
  return null;
}

function buildBaseballDiamond(comp, leagueCode){
  if(String(leagueCode||'').toLowerCase()!=='mlb') return null;
  const situation=comp?.situation;
  if(!situation) return null;
  const bases=parseBaseOccupancy(situation);
  const outs=clampStatValue(situation.outs, 0, 3);
  const balls=clampStatValue(situation.balls, 0, 3);
  const strikes=clampStatValue(situation.strikes, 0, 2);
  const pitchCount=parsePitchTotal(situation.pitchCount);
  const lastEvent=situation.lastPlay?.text || '';
  const scoring=Boolean(situation.lastPlay?.scoringPlay || /scores|homered|walk-off|grand slam/i.test(lastEvent));
  const countText=situation.count?.displayValue || situation.count || situation.pitchCountText || null;
  const hasContext=bases.first || bases.second || bases.third || outs!=null || balls!=null || strikes!=null || pitchCount!=null || lastEvent;
  if(!hasContext) return null;
  return {
    bases,
    outs,
    balls,
    strikes,
    pitchCount,
    countText:countText || (pitchCount!=null ? `${pitchCount} pitches` : ''),
    batter:situation.batter?.athlete?.displayName || situation.batter?.athlete?.shortName || null,
    pitcher:situation.pitcher?.athlete?.displayName || situation.pitcher?.athlete?.shortName || null,
    description:situation.onBase?.long || situation.onBase?.short || situation.onBase?.text || '',
    lastEvent,
    scoring,
    inning:comp?.status?.period ?? situation.lastPlay?.period?.number ?? null,
    half:extractHalfInning(comp)
  };
}

async function getShotChart(gameId){
  const key=String(gameId||'').trim();
  if(!key) return { gameId:'', shots:[], teams:[], players:[], periods:[] };
  const cached=shotChartCache.get(key);
  if(cached && Date.now()-cached.ts<SHOT_CACHE_TTL){
    return cached.data;
  }
  const payload=await fetchShotChartRemote(key);
  shotChartCache.set(key,{ ts:Date.now(), data:payload });
  return payload;
}

async function fetchShotChartRemote(gameId){
  const base='https://site.api.espn.com/apis/site/v2/sports/basketball/nba';
  try{
    const [pbp, summary]=await Promise.all([
      fetchJSON(`${base}/playbyplay?event=${gameId}`).catch(()=>null),
      fetchJSON(`${base}/summary?event=${gameId}`).catch(()=>null)
    ]);
    const competition=pbp?.game?.competitions?.[0] || summary?.header?.competitions?.[0] || summary?.events?.[0]?.competitions?.[0] || pbp?.competitions?.[0] || {};
    const teams=(competition?.competitors||[]).map(mapShotTeam).filter(team=>team.id);
    const colorMap=new Map(teams.map(team=>[team.id, team.color||'#58a6ff']));
    const plays=collectShotPlays(pbp) || [];
    const shots=plays.map(play=>normalizeShotEntry(play, colorMap)).filter(Boolean);
    const players=buildShotPlayerMeta(shots);
    const periods=[...new Set(shots.map(s=>s.period).filter(Boolean))].sort((a,b)=>a-b);
    return {
      gameId,
      shots,
      teams,
      players,
      periods,
      fetchedAt:Date.now()
    };
  }catch(err){
    console.error('Shot chart fetch failed', gameId, err.message);
    return { gameId, shots:[], teams:[], players:[], periods:[], fetchedAt:Date.now(), error:'unavailable' };
  }
}

function mapShotTeam(entry){
  const team=entry?.team || entry;
  const id=team?.id || entry?.id;
  if(!id) return { id:null };
  const colorRaw=team.color || entry.color || '58a6ff';
  const normalizedColor=colorRaw.startsWith('#') ? colorRaw : `#${colorRaw}`;
  const homeAway=(entry?.homeAway || team?.homeAway || '').toLowerCase();
  return {
    id:String(id),
    name:team.displayName || team.shortDisplayName || team.name || '',
    abbreviation:team.abbreviation || entry.abbreviation || '',
    color:normalizedColor,
    alternateColor:team.alternateColor ? `#${team.alternateColor.replace('#','')}` : null,
    isHome:homeAway==='home'
  };
}

function collectShotPlays(payload){
  const plays=[];
  const dig=list=>Array.isArray(list)&&list.forEach(item=>{
    if(Array.isArray(item?.plays)){
      item.plays.forEach(play=>plays.push(play));
    }
  });
  dig(payload?.drives);
  dig(payload?.game?.drives);
  if(plays.length) return plays;
  if(Array.isArray(payload?.plays)) return payload.plays;
  const comp = payload?.game?.competitions?.[0];
  if(Array.isArray(comp?.plays)) return comp.plays;
  const pbp = payload?.game?.playByPlay?.plays;
  if(Array.isArray(pbp)) return pbp;
  return [];
}

function normalizeShotEntry(play, colorMap){
  if(!isShotAttempt(play)) return null;
  const coords=normalizeShotCoordinates(play.coordinates || play.coordinate || play.position || play?.shotChart?.coordinates);
  if(!coords) return null;
  const athlete=getShotAthlete(play);
  const teamId=String(play.team?.id || athlete?.teamId || '');
  const color=colorMap.get(teamId) || '#58a6ff';
  const result=parseShotResult(play);
  return {
    id:String(play.id || `${play.sequenceNumber||Date.now()}-${Math.random().toString(36).slice(2,7)}`),
    teamId:teamId || null,
    teamAbbr:play.team?.abbreviation || play.team?.shortDisplayName || play.team?.displayName || '',
    teamColor:color,
    playerId:athlete?.id || null,
    player:athlete?.name || 'Unknown',
    period:play.period?.number || play.periodNumber || 0,
    clock:play.clock?.displayValue || play.time || '',
    result,
    isMake:result==='make',
    shotValue:parseShotValue(play),
    description:play.text || '',
    x:coords.x,
    y:coords.y
  };
}

function isShotAttempt(play){
  if(!play) return false;
  if(play.shotResult || play.shotValue!=null) return true;
  if(play.scoringPlay && play.scoreValue) return true;
  const typeName=String(play?.type?.text || play?.type?.description || '').toLowerCase();
  if(typeName.includes('shot') || typeName.includes('jumper') || typeName.includes('layup') || typeName.includes('dunk')) return true;
  const desc=String(play.text||'').toLowerCase();
  return /shot|jumper|layup|dunk|three/i.test(desc);
}

function parseShotResult(play){
  const raw=String(play?.shotResult || play?.result || '').toLowerCase();
  if(raw.includes('made') || raw==='make') return 'make';
  if(raw.includes('miss')) return 'miss';
  const desc=String(play?.text||'').toLowerCase();
  if(/miss|blocked/.test(desc)) return 'miss';
  if(/makes|hits|scores|dunk|tip-in/.test(desc)) return 'make';
  return 'unknown';
}

function parseShotValue(play){
  if(typeof play?.shotValue==='number') return Number(play.shotValue);
  if(typeof play?.scoreValue==='number') return Number(play.scoreValue);
  const stat=(play?.statistics||[]).find(entry=>entry.type==='fieldGoal');
  if(typeof stat?.value==='number') return Number(stat.value);
  const desc=String(play?.text||'').toLowerCase();
  if(/3-pt|three/.test(desc)) return 3;
  return 2;
}

function getShotAthlete(play){
  const participant=(play?.participants||[]).find(part=>part?.athlete) || null;
  const athlete=participant?.athlete;
  if(!athlete){
    const player=play?.athletesInvolved?.[0];
    if(player){
      return { id:String(player.id||''), name:player.displayName || player.shortName || 'Player', teamId: String(player.teamId||'') };
    }
    return null;
  }
  return {
    id:athlete.id ? String(athlete.id) : null,
    name:athlete.displayName || athlete.shortName || athlete.lastName || 'Player',
    teamId:participant.teamId ? String(participant.teamId) : (play.team?.id ? String(play.team.id): null)
  };
}

function normalizeShotCoordinates(coord){
  if(!coord) return null;
  let x=coord.x ?? coord.X ?? coord.lon ?? coord.longitude ?? coord.lng;
  let y=coord.y ?? coord.Y ?? coord.lat ?? coord.latitude;
  x=Number(x);
  y=Number(y);
  if(!Number.isFinite(x) || !Number.isFinite(y)) return null;
  const clampedX=Math.max(-COURT_HALF_WIDTH, Math.min(COURT_HALF_WIDTH, x));
  const clampedY=Math.max(0, Math.min(COURT_LENGTH, y));
  const ratioX=(clampedX + COURT_HALF_WIDTH)/(COURT_HALF_WIDTH*2);
  const ratioY=clampedY/COURT_LENGTH;
  return {
    x:Number(ratioX.toFixed(4)),
    y:Number(ratioY.toFixed(4))
  };
}

function buildShotPlayerMeta(shots){
  const map=new Map();
  shots.forEach(shot=>{
    if(!shot.playerId) return;
    if(!map.has(shot.playerId)){
      map.set(shot.playerId,{ id:shot.playerId, name:shot.player, teamId:shot.teamId, shots:0, makes:0 });
    }
    const entry=map.get(shot.playerId);
    entry.shots+=1;
    if(shot.isMake) entry.makes+=1;
  });
  return [...map.values()].sort((a,b)=>b.shots-a.shots);
}


async function getDriveChart(gameId){
  const key=String(gameId||'').trim();
  if(!key) return { gameId:'', drives:[], teams:[], fetchedAt:Date.now() };
  const cached=driveChartCache.get(key);
  if(cached && Date.now()-cached.ts<DRIVE_CACHE_TTL){
    return cached.data;
  }
  const payload=await fetchDriveChartRemote(key);
  driveChartCache.set(key,{ ts:Date.now(), data:payload });
  return payload;
}

async function fetchDriveChartRemote(gameId){
  const base='https://site.api.espn.com/apis/site/v2/sports/football/nfl';
  try{
    const [pbp, summary]=await Promise.all([
      fetchJSON(`${base}/playbyplay?event=${gameId}`).catch(()=>null),
      fetchJSON(`${base}/summary?event=${gameId}`).catch(()=>null)
    ]);
    const competition=pbp?.game?.competitions?.[0] || summary?.header?.competitions?.[0] || summary?.events?.[0]?.competitions?.[0] || pbp?.competitions?.[0] || {};
    const teams=(competition?.competitors||[]).map(mapShotTeam).filter(team=>team.id);
    const teamsById=new Map(teams.map(team=>[team.id, team]));
    const teamsByAbbr=new Map(teams.map(team=>[(team.abbreviation||'').toUpperCase(), team]));
    const drivesList=collectDriveList(pbp);
    const normalized=drivesList.map((drive, index)=>normalizeDriveEntry(drive, teamsById, teamsByAbbr, index)).filter(Boolean);
    return {
      gameId,
      fetchedAt:Date.now(),
      teams,
      drives:normalized
    };
  }catch(err){
    console.error('Drive chart fetch failed', gameId, err.message);
    return { gameId, drives:[], teams:[], fetchedAt:Date.now(), error:'unavailable' };
  }
}

function collectDriveList(payload){
  if(!payload) return [];
  const previous=payload?.drives?.previous || [];
  const current=payload?.drives?.current || [];
  if(Array.isArray(payload?.drives)) return payload.drives;
  return [...previous, ...current];
}

function normalizeDriveEntry(drive, teamsById, teamsByAbbr, index){
  if(!drive) return null;
  const offenseId=String(drive.team?.id || drive.offense?.id || drive.start?.team?.id || '');
  if(!offenseId) return null;
  const offenseTeam=teamsById.get(offenseId) || {
    id:offenseId,
    abbreviation:drive.team?.abbreviation || '',
    color:'#58a6ff',
    isHome:false
  };
  const opponent=[...teamsById.values()].find(team=>team.id!==offenseId) || null;
  const startYards=computeDriveYards(drive.start, offenseTeam, opponent, teamsByAbbr);
  const endYards=computeDriveYards(drive.end, offenseTeam, opponent, teamsByAbbr);
  const result=drive.result || drive.displayResult || drive.end?.displayResult || '';
  const scoring=Boolean(drive.isScore || /touchdown|field goal|safety/i.test(result));
  const plays=drive.plays || drive.offensivePlays || 0;
  const yards=drive.yards || drive.yardsGained || drive.netYards || (typeof startYards==='number' && typeof endYards==='number' ? Math.round(endYards-startYards) : null);
  const description=drive.description || drive.detail || drive.shortDetail || '';
  const possession=drive.timeOfPossession || drive.duration || '';
  return {
    id:String(drive.id || `drive-${index}`),
    index,
    teamId:offenseId,
    teamAbbr:offenseTeam.abbreviation || '',
    teamColor:offenseTeam.color || '#58a6ff',
    teamIsHome:Boolean(offenseTeam.isHome),
    startYards,
    endYards,
    startText:drive.start?.yardLine || drive.start?.text || '',
    endText:drive.end?.yardLine || drive.end?.text || '',
    quarter:drive.start?.period || drive.period?.number || null,
    clock:drive.start?.clock?.displayValue || drive.start?.clock || '',
    result:result || 'Drive',
    plays,
    yards,
    scoring,
    description,
    possession,
    endResult:drive.end?.result || '',
    turnover:result ? /interception|fumble|downs|punt/i.test(result) : false
  };
}

function computeDriveYards(marker, offenseTeam, opponentTeam, teamsByAbbr){
  if(!marker) return null;
  const yardsToGoal=Number(marker.yardsToGoal ?? marker.yardsFromGoal);
  if(Number.isFinite(yardsToGoal)){
    return clampYard(100 - yardsToGoal);
  }
  const text=marker.yardLine || marker.yardLineText || marker.downDistanceText || marker.text || '';
  const match=text.toUpperCase().match(/([A-Z]{2,4})\s*(\d{1,2})/);
  let yardTeam=null;
  let yard=NaN;
  if(match){
    const code=match[1];
    const val=match[2];
    if(code && !/^\d+$/.test(code)){
      yardTeam=code;
    }
    yard=Number(val);
  }else{
    const numberMatch=text.match(/(\d{1,2})\s*yard/i);
    if(numberMatch){
      yard=Number(numberMatch[1]);
    }
  }
  if(!Number.isFinite(yard)){
    const numeric=Number(marker.yards);
    if(Number.isFinite(numeric)) yard=numeric;
  }
  if(!Number.isFinite(yard)) return null;
  const offenseAbbr=(offenseTeam?.abbreviation||'').toUpperCase();
  const opponentAbbr=(opponentTeam?.abbreviation||'').toUpperCase();
  const prefix=(yardTeam || offenseAbbr || opponentAbbr || '').toUpperCase();
  if(prefix==='MID' || prefix==='50'){
    return 50;
  }
  if(prefix===offenseAbbr){
    return clampYard(yard);
  }
  if(prefix===opponentAbbr && opponentAbbr){
    return clampYard(100-yard);
  }
  if(prefix && teamsByAbbr && teamsByAbbr.has(prefix) && prefix!==offenseAbbr){
    return clampYard(100-yard);
  }
  return clampYard(yard);
}

function clampYard(value){
  if(!Number.isFinite(value)) return null;
  return Math.max(0, Math.min(100, value));
}

function normalizeStandingsRows(data){
  const rows=(data?.children||[]).flatMap(child=>child?.standings?.entries||[]);
  return rows.map(entry=>{
    const stats=entry.stats||[];
    const stat=type=>stats.find(s=>s.type===type||s.name===type);
    return {
      id:entry.team?.id,
      name:entry.team?.displayName||entry.team?.name,
      record:`${stat('wins')?.value||0}-${stat('losses')?.value||0}`,
      pct:Number(stat('winpercent')?.value)||0,
      streak:stat('streak')?.displayValue||'',
      diff:stat('pointdifferential')?.displayValue||''
    };
  }).sort((a,b)=>b.pct-a.pct);
}

function sortGames(a,b){
  const weight=state=>state==='in'?0:state==='pre'?1:2;
  const diff=weight(a.status.state)-weight(b.status.state);
  if(diff!==0) return diff;
  return (a.start?.getTime()||Infinity)-(b.start?.getTime()||Infinity);
}

function getSubscriptions(owner){
  if(!owner) return [];
  if(!notifyStore.has(owner)) notifyStore.set(owner, []);
  return notifyStore.get(owner);
}

function upsertSubscription(owner, targetType, targetId){
  const list=getSubscriptions(owner);
  const existing=list.find(sub=>sub.targetType===targetType && sub.targetId===targetId);
  if(existing) return existing;
  const normalized=normalizeSubscription({ targetType, targetId });
  if(!normalized) return null;
  list.push(normalized);
  return normalized;
}

function removeSubscription(owner, targetType, targetId){
  const list=getSubscriptions(owner);
  const next=list.filter(sub=>!(sub.targetType===targetType && sub.targetId===targetId));
  notifyStore.set(owner,next);
  return list.length!==next.length;
}

function evaluateReminders(owner, games){
  if(!owner) return [];
  const list=getSubscriptions(owner);
  if(!list.length) return [];
  const alerts=[];
  const now=Date.now();
  let dirty=false;
  list.forEach(sub=>{
    const matches=sub.targetType==='game'
      ? games.filter(g=>String(g.id)===sub.targetId)
      : games.filter(g=>String(g.home?.id)===sub.targetId || String(g.away?.id)===sub.targetId);
    if(!matches.length) return;
    matches.forEach(game=>{
      const title=`${game.away.short} @ ${game.home.short}`;
      const summary={ gameId:game.id, league:game.league, title, targetType:sub.targetType, targetId:sub.targetId };
      const state=(game.status?.state||'').toLowerCase();
      const startTs=game.start?.getTime();
      if(startTs){
        const mins=Math.round((startTs-now)/60000);
        if(state==='pre' && mins<=30 && !sub.lastNotified.start){
          alerts.push({ ...summary, kind:'start', message:`Kickoff soon: ${title} in ${Math.max(mins,0)} min` });
          sub.lastNotified.start=true;
          dirty=true;
        }
      }
      const scoreSig=`${game.away.score ?? '—'}-${game.home.score ?? '—'}`;
      if(state==='in'){
        if(sub.lastNotified.scoreSig && sub.lastNotified.scoreSig!==scoreSig){
          alerts.push({ ...summary, kind:'score', message:`Score update: ${game.away.short} ${game.away.score} - ${game.home.short} ${game.home.score}` });
        }
        if(sub.lastNotified.scoreSig!==scoreSig){
          sub.lastNotified.scoreSig=scoreSig;
          dirty=true;
        }
      }
      if(state==='post' && !sub.lastNotified.final){
        alerts.push({ ...summary, kind:'final', message:`Final: ${scoreSig}` });
        sub.lastNotified.final=true;
        dirty=true;
      }
    });
  });
  if(dirty) persistNotifyStore().catch(err=>console.error('Notify persist failed', err.message));
  return alerts;
}

async function fetchSocialPostsRemote(query, options={}){
  const url=new URL('https://www.reddit.com/search.json');
  url.searchParams.set('q', query);
  url.searchParams.set('sort','new');
  url.searchParams.set('limit', String(options.limit || 10));
  if(options.timeRange) url.searchParams.set('t', options.timeRange);
  const data=await fetchJSON(url.toString());
  const since=typeof options.since==='number' ? options.since : Date.now() - 6*60*60*1000;
  const keywords=(options.keywords||[]).map(word=>word.toLowerCase()).filter(Boolean);
  return (data?.data?.children||[]).map(child=>{
    const item=child?.data;
    if(!item?.id) return null;
    const thumbnail=item.thumbnail && item.thumbnail.startsWith('http') ? item.thumbnail : '';
    const summary=item.title || item.selftext?.slice(0,160) || '';
    const fullText=`${item.title||''} ${item.selftext||''}`.toLowerCase();
    return {
      id:item.id,
      author:item.author || 'Redditor',
      text:summary,
      url:item.permalink ? `https://www.reddit.com${item.permalink}` : '',
      image:thumbnail,
      created:item.created_utc ? item.created_utc*1000 : Date.now(),
      source:'Reddit'
    };
  }).filter(post=>{
    if(!post) return false;
    if(since && post.created && post.created < since) return false;
    if(keywords.length){
      const text=(post.text||'').toLowerCase();
      const matches=keywords.some(keyword=>{
        const clean=keyword.replace(/^#/, '');
        return text.includes(clean);
      });
      if(!matches) return false;
    }
    return true;
  });
}

function sanitizeFanPost(text){
  if(!text) return '';
  return String(text).trim().slice(0,500);
}

function sanitizeFanAuthor(name){
  if(!name) return 'Fan';
  return String(name).trim().slice(0,32) || 'Fan';
}

const app = express();
app.use(cors());
app.use(express.json({ limit:'1mb' }));

app.post('/api/auth/signup', async (req,res)=>{
  const { name, email, password } = req.body||{};
  if(!name || !email || !password) return res.status(400).json({ error:'Name, email, and password required' });
  const normalizedEmail=email.toLowerCase();
  const data=await readData();
  if(data.users.some(u=>u.email===normalizedEmail)) return res.status(400).json({ error:'Email already registered' });
  const salt=crypto.randomBytes(16).toString('hex');
  const passwordHash=hashPassword(password,salt);
  const user={ id:crypto.randomUUID(), name:name.trim(), email:normalizedEmail, passwordHash, salt, createdAt:new Date().toISOString() };
  data.users.push(user);
  const profileRecord={
    favorites:[],
    watchlist:[],
    preferences:{ ...DEFAULT_PREFERENCES },
    details:defaultProfileDetails()
  };
  data.profiles[user.id]=profileRecord;
  await writeData(data);
  const token=createToken();
  tokens.set(token, user.id);
  res.json({
    token,
    user:safeUser(user),
    favorites:[],
    watchlist:[],
    preferences:{ ...DEFAULT_PREFERENCES },
    details:profileRecord.details,
    profile:buildPublicProfile(user, profileRecord)
  });
});

app.post('/api/auth/login', async (req,res)=>{
  const { email, password } = req.body||{};
  if(!email || !password) return res.status(400).json({ error:'Email and password required' });
  const data=await readData();
  const user=data.users.find(u=>u.email===email.toLowerCase());
  if(!user) return res.status(401).json({ error:'Invalid credentials' });
  const hashed=hashPassword(password,user.salt);
  if(hashed!==user.passwordHash) return res.status(401).json({ error:'Invalid credentials' });
  const token=createToken();
  tokens.set(token, user.id);
  const profile=ensureProfileRecord(data, user);
  await writeData(data);
  res.json({
    token,
    user:safeUser(user),
    favorites:profile.favorites,
    watchlist:profile.watchlist||[],
    preferences:profile.preferences,
    details:profile.details,
    profile:buildPublicProfile(user, profile)
  });
});

function authMiddleware(req,res,next){
  const header=req.headers.authorization||'';
  const token=header.replace('Bearer','').trim();
  const userId=tokens.get(token);
  if(!userId) return res.status(401).json({ error:'Unauthorized' });
  req.userId=userId;
  req.token=token;
  next();
}

app.get('/api/profile', authMiddleware, async (req,res)=>{
  const data=await readData();
  const user=data.users.find(u=>u.id===req.userId);
  if(!user) return res.status(404).json({ error:'User not found' });
  const profile=ensureProfileRecord(data, user);
  await writeData(data);
  res.json({
    user:safeUser(user),
    favorites:profile.favorites,
    watchlist:profile.watchlist,
    preferences:profile.preferences,
    details:profile.details,
    profile:buildPublicProfile(user, profile)
  });
});

app.post('/api/profile/details', authMiddleware, async (req,res)=>{
  const data=await readData();
  const user=data.users.find(u=>u.id===req.userId);
  if(!user) return res.status(404).json({ error:'User not found' });
  const profile=ensureProfileRecord(data, user);
  const incoming=req.body||{};
  const normalized=normalizeProfileDetails({
    ...profile.details,
    avatar:incoming.avatar,
    bio:incoming.bio,
    favoriteTeams:Array.isArray(incoming.favoriteTeams)?incoming.favoriteTeams:incoming.favoriteTeams?.split?.(',')||[]
  });
  profile.details={ ...normalized, badges:profile.details.badges };
  data.profiles[user.id]=profile;
  await writeData(data);
  res.json({ success:true, details:profile.details, profile:buildPublicProfile(user, profile) });
});

app.get('/api/profile/:id', async (req,res)=>{
  const targetId=req.params.id;
  const data=await readData();
  const user=data.users.find(u=>u.id===targetId);
  if(!user) return res.status(404).json({ error:'Profile not found' });
  const profile=normalizeProfileRecord(data.profiles[user.id], user);
  res.json({ profile:buildPublicProfile(user, profile) });
});

app.post('/api/profile/favorites', authMiddleware, async (req,res)=>{
  if(!Array.isArray(req.body?.favorites)) return res.status(400).json({ error:'Favorites array required' });
  const favorites = normalizeFavoriteList(req.body.favorites);
  const data=await readData();
  data.profiles[req.userId]=data.profiles[req.userId]||{};
  data.profiles[req.userId].favorites=favorites;
  await writeData(data);
  logAnalyticsEvent('favorite:update',{ userId:req.userId, count:favorites.length });
  res.json({ success:true, favorites });
});

app.post('/api/profile/watchlist', authMiddleware, async (req,res)=>{
  const watchlist = Array.isArray(req.body?.watchlist) ? req.body.watchlist.filter(item=>item && item.url) : null;
  if(!watchlist) return res.status(400).json({ error:'Watchlist array required' });
  const data=await readData();
  data.profiles[req.userId]=data.profiles[req.userId]||{};
  data.profiles[req.userId].watchlist=watchlist;
  await writeData(data);
  logAnalyticsEvent('watchlist:update',{ userId:req.userId, count:watchlist.length });
  res.json({ success:true });
});

app.post('/api/profile/preferences', authMiddleware, async (req,res)=>{
  const prefs=req.body?.preferences;
  if(!prefs || typeof prefs!=='object') return res.status(400).json({ error:'Preferences payload required' });
  const biasRaw=String(prefs.discoveryBias||'').toLowerCase();
  const bias=['favorites','mixed','hot'].includes(biasRaw)?biasRaw:DEFAULT_PREFERENCES.discoveryBias;
  const normalized={
    leagues:Array.isArray(prefs.leagues)&&prefs.leagues.length ? prefs.leagues.map(l=>String(l).toLowerCase()) : DEFAULT_PREFERENCES.leagues,
    provider: (prefs.provider || DEFAULT_PREFERENCES.provider).toLowerCase(),
    discoveryBias: bias
  };
  const data=await readData();
  data.profiles[req.userId]=data.profiles[req.userId]||{};
  data.profiles[req.userId].preferences=normalized;
  await writeData(data);
  res.json({ success:true, preferences:normalized });
});

app.post('/api/device/pair', async (req,res)=>{
  const payload=buildPairPayload(req.body||{});
  cleanupDevicePairs();
  const code=generatePairCode();
  const entry={
    code,
    createdAt:Date.now(),
    expiresAt:Date.now()+DEVICE_PAIR_TTL,
    payload
  };
  devicePairStore.set(code, entry);
  await persistDevicePairs();
  const host=req.get('host');
  const pairUrl=`${req.protocol}://${host}/?pair=${code}`;
  res.json({ code, expiresAt:entry.expiresAt, pairUrl });
});

app.get('/api/device/pair/:code', async (req,res)=>{
  cleanupDevicePairs();
  const code=String(req.params.code||'').toUpperCase();
  if(!devicePairStore.has(code)){
    return res.status(404).json({ error:'Pairing code not found or expired' });
  }
  const entry=devicePairStore.get(code);
  devicePairStore.delete(code);
  await persistDevicePairs();
  res.json({ code, ...entry.payload });
});

app.get('/api/chat',async (req,res)=>{
  const gameId=req.query.gameId || 'global';
  const messages=await getChatHistory(gameId);
  res.json({ messages });
});

app.post('/api/chat', async (req,res)=>{
  const { gameId='global', author='Guest', body='' } = req.body||{};
  const clean=sanitizeMessage(body);
  if(!clean) return res.status(400).json({ error:'Message rejected' });
  const key=req.ip||'anon';
  const now=Date.now();
  const last=chatRateLimiter.get(key)||0;
  if(now-last<RATE_WINDOW) return res.status(429).json({ error:'Slow down' });
  chatRateLimiter.set(key, now);
  await appendChatMessage(gameId,{ author:author?.slice(0,18)||'Guest', body:clean, ts:now });
  logAnalyticsEvent('chat:message',{ gameId, author, source:'http', ip:req.ip });
  res.json({ ok:true });
});

app.get('/api/polls/:gameId',async (req,res)=>{
  const poll=await getPollSnapshot(req.params.gameId);
  if(!poll) return res.status(404).json({ error:'Poll not found' });
  res.json(poll);
});

app.post('/api/polls/:gameId', async (req,res)=>{
  const { question, options } = req.body||{};
  if(!question || !Array.isArray(options) || options.length<2){
    return res.status(400).json({ error:'Question and at least two options required' });
  }
  const normalized=options.map(opt=>({ id:String(opt.id||crypto.randomUUID()), label:String(opt.label||'Option'), count:Number(opt.count)||0 }));
  const poll={ question:String(question), options:normalized, votes:{}, closed:false };
  await savePollSnapshot(req.params.gameId, poll);
  res.json(poll);
});

app.post('/api/polls/:gameId/vote', async (req,res)=>{
  const poll=await getPollSnapshot(req.params.gameId);
  if(!poll) return res.status(404).json({ error:'Poll not found' });
  if(poll.closed) return res.status(400).json({ error:'Poll closed' });
  const optionId=req.body?.optionId;
  const option=poll.options.find(opt=>opt.id===optionId);
  if(!option) return res.status(400).json({ error:'Invalid option' });
  const key=req.userId || req.ip || crypto.randomUUID();
  if(poll.votes[key]) return res.status(409).json({ error:'Already voted' });
  poll.votes[key]=optionId;
  option.count+=1;
  await savePollSnapshot(req.params.gameId, poll);
  logAnalyticsEvent('poll:vote',{ gameId:req.params.gameId, optionId, userId:req.userId||null, ip:req.ip });
  res.json({ poll });
});

app.post('/api/analytics', (req,res)=>{
  const { type, payload={} } = req.body||{};
  if(!type) return res.status(400).json({ error:'type required' });
  const userId=resolveUserFromRequest(req);
  const guestId=req.body?.guestId || `guest:${req.ip||'anon'}`;
  logAnalyticsEvent(type,{ ...payload, userId:userId||null, guestId:userId?undefined:guestId });
  res.json({ success:true });
});

app.get('/api/admin/metrics',(req,res)=>{
  if(!isAdminRequest(req)) return res.status(403).json({ error:'Forbidden' });
  res.json(buildAnalyticsSummary());
});

app.get('/api/social/posts', async (req,res)=>{
  const { gameId='', teamId='', q='', teamName='', league='' } = req.query||{};
  const tagList=(req.query.tags||'').split(',').map(v=>v.trim()).filter(Boolean);
  const query=(q || teamName || req.query.team || '').trim();
  if(!query && !teamId && !gameId && !tagList.length){
    return res.status(400).json({ error:'Provide q, teamName, teamId, tags, or gameId' });
  }
  const searchParts=[];
  if(query) searchParts.push(query);
  if(tagList.length){
    searchParts.push(`(${tagList.map(tag=>`"${tag}"`).join(' OR ')})`);
  }
  if(league) searchParts.push(league);
  const searchTerm=searchParts.join(' ') || teamId || gameId || 'sports';
  const sinceParam=Number(req.query.since);
  const since=Number.isFinite(sinceParam) && sinceParam>0 ? sinceParam : Date.now()-6*60*60*1000;
  const keywords=tagList.map(tag=>tag.replace(/^#/, '').toLowerCase());
  const sinceBucket=Math.floor(since/(60*60*1000));
  const cacheKey=`${searchTerm.toLowerCase()}:${gameId}:${teamId}:${sinceBucket}`;
  let posts=[];
  const cached=socialCache.get(cacheKey);
  if(cached && Date.now()-cached.ts<SOCIAL_CACHE_TTL){
    posts=cached.posts;
  }else{
    try{
      posts=await fetchSocialPostsRemote(searchTerm,{ keywords, since, limit:10, timeRange:'day' });
    }catch(err){
      console.error('Social fetch failed', err.message);
      posts=[];
    }
    socialCache.set(cacheKey,{ posts, ts:Date.now() });
  }
  const targetKey=socialTargetKey({ teamId, gameId, query:searchTerm });
  res.json({ posts, fanPosts:getFanPosts(targetKey), targetKey });
});

app.post('/api/social/fanposts', async (req,res)=>{
  const { targetKey, teamId, gameId, query, author, body } = req.body||{};
  const key=targetKey || socialTargetKey({ teamId, gameId, query });
  const cleanBody=sanitizeFanPost(body);
  if(!key) return res.status(400).json({ error:'targetKey or identifiers required' });
  if(!cleanBody) return res.status(400).json({ error:'Message required' });
  const entry={
    id:crypto.randomUUID(),
    author:sanitizeFanAuthor(author || req.body?.name || 'Fan'),
    body:cleanBody,
    ts:Date.now(),
    source:'fan'
  };
  addFanPost(key, entry);
  await persistFanPosts();
  res.json({ success:true, post:entry, fanPosts:getFanPosts(key), targetKey:key });
});

app.get('/api/history/:league/:year', async (req,res)=>{
  const league=(req.params.league||'').toLowerCase();
  const rawYear=String(req.params.year||'').replace(/[^0-9]/g,'');
  if(!rawYear) return res.status(400).json({ error:'Year required' });
  if(!HISTORY_CONFIG[league]) return res.status(400).json({ error:'League not supported' });
  const cached=historyStore?.[league]?.[rawYear];
  if(cached){
    return res.json({ ...cached.data, cached:true, cachedAt:cached.ts });
  }
  try{
    const payload=await fetchHistorySeason(league, Number(rawYear));
    cacheHistorySeason(league, rawYear, payload);
    res.json({ ...payload, cached:false });
  }catch(err){
    console.error('History fetch failed', err.message);
    res.status(500).json({ error:'Unable to load historical season' });
  }
});

app.get('/api/clips',(req,res)=>{
  const limit=Math.min(50, Math.max(1, parseInt(req.query.limit,10)||12));
  const filtered=clipStore.slice(-limit).reverse();
  res.json({ clips:filtered });
});

app.post('/api/clips', async (req,res)=>{
  const { title, sourceType, sourceId, url, startTime=0, endTime=null, meta={}, author='Fan' } = req.body||{};
  if(!title || !url || !sourceType){
    return res.status(400).json({ error:'title, sourceType, and url are required' });
  }
  const start=Number(startTime)||0;
  const end=endTime!=null?Number(endTime):null;
  if(end!=null && end<=start){
    return res.status(400).json({ error:'endTime must be greater than startTime' });
  }
  const entry={
    id:crypto.randomUUID(),
    title:String(title).slice(0,80),
    sourceType:String(sourceType).toLowerCase(),
    sourceId:sourceId?String(sourceId):null,
    url:String(url),
    startTime:start,
    endTime:end,
    meta:meta && typeof meta==='object' ? meta : {},
    author:String(author).slice(0,40),
    createdAt:Date.now()
  };
  clipStore.push(entry);
  if(clipStore.length>200) clipStore=clipStore.slice(-200);
  await persistClips();
  res.json({ clip:entry });
});

app.post('/api/jobs/recap', async (req,res)=>{
  const { gameId } = req.body||{};
  if(!gameId) return res.status(400).json({ error:'gameId required' });
  try{
    const result=await enqueueRecapJob(gameId);
    res.json(result);
  }catch(err){
    console.error('Recap job enqueue failed', err.message);
    res.status(502).json({ error:'Unable to enqueue recap job' });
  }
});

app.post('/api/jobs/analytics', async (req,res)=>{
  const payload={ window:req.body?.window || 'latest' };
  try{
    const result=await enqueueAnalyticsJob(payload);
    res.json(result);
  }catch(err){
    console.error('Analytics job enqueue failed', err.message);
    res.status(502).json({ error:'Unable to enqueue analytics job' });
  }
});

app.post('/api/jobs/clips', async (req,res)=>{
  const clipIds=Array.isArray(req.body?.clipIds) ? req.body.clipIds : [];
  try{
    const result=await enqueueClipIndexJob({ clipIds:clipIds.map(id=>String(id)) });
    res.json(result);
  }catch(err){
    console.error('Clip job enqueue failed', err.message);
    res.status(502).json({ error:'Unable to enqueue clip job' });
  }
});

app.post('/api/compare', async (req,res)=>{
  const { type='team', a, b } = req.body||{};
  if(!a || !b) return res.status(400).json({ error:'Two ids required' });
  try{
    if(type==='player'){
      const [profileA, profileB]=await Promise.all([getPlayerProfile(a), getPlayerProfile(b)]);
      if(!profileA || !profileB) return res.status(404).json({ error:'Player not found' });
      const entityA=buildPlayerComparison(profileA);
      const entityB=buildPlayerComparison(profileB);
      res.json({
        type:'player',
        summary:buildComparisonSummary('player', entityA, entityB),
        entities:[entityA, entityB],
        stats:{
          a:entityA.stats,
          b:entityB.stats
        },
        injuries:{
          a:[],
          b:[]
        }
      });
    }else{
      const [profileA, profileB]=await Promise.all([getTeamProfile(a), getTeamProfile(b)]);
      if(!profileA || !profileB) return res.status(404).json({ error:'Team not found' });
      const contextA=findRecentTeamContext(a);
      const contextB=findRecentTeamContext(b);
      const entityA=buildTeamComparison(profileA, contextA);
      const entityB=buildTeamComparison(profileB, contextB);
      res.json({
        type:'team',
        summary:buildComparisonSummary('team', entityA, entityB),
        entities:[entityA, entityB],
        odds:buildCompareOdds(contextA, contextB),
        stats:{
          a:entityA.stats||[],
          b:entityB.stats||[]
        },
        injuries:{
          a:gatherTeamInjuries(profileA, contextA),
          b:gatherTeamInjuries(profileB, contextB)
        },
        rankings:{
          a:entityA.rankings||[],
          b:entityB.rankings||[]
        }
      });
    }
  }catch(err){
    console.error('Compare failed', err.message);
    res.status(502).json({ error:'Comparison failed' });
  }
});

app.get('/api/predictions/leagues', authMiddleware, async (req,res)=>{
  const data=await readData();
  const user=data.users.find(u=>u.id===req.userId);
  if(!user) return res.status(404).json({ error:'User not found' });
  const leagues=getUserLeagues(user.id).map(league=>publicLeagueData(league, user.id));
  res.json({ leagues });
});

app.post('/api/predictions/leagues', authMiddleware, async (req,res)=>{
  const rawName=(req.body?.name||'').toString().trim();
  if(!rawName) return res.status(400).json({ error:'League name required' });
  const name=rawName.slice(0,80);
  const data=await readData();
  const user=data.users.find(u=>u.id===req.userId);
  if(!user) return res.status(404).json({ error:'User not found' });
  const ts=Date.now();
  const league={
    id:crypto.randomUUID(),
    name,
    ownerId:user.id,
    code:generateInviteCode(),
    createdAt:ts,
    members:[{ userId:user.id, name:user.name, joinedAt:ts }]
  };
  leagueStore.push(league);
  await persistPredictions();
  res.json({ league:publicLeagueData(league, user.id) });
});

app.post('/api/predictions/leagues/join', authMiddleware, async (req,res)=>{
  const code=(req.body?.code||'').toString().trim().toUpperCase();
  if(!code) return res.status(400).json({ error:'Invite code required' });
  const league=findLeagueByCode(code);
  if(!league) return res.status(404).json({ error:'League not found' });
  const data=await readData();
  const user=data.users.find(u=>u.id===req.userId);
  if(!user) return res.status(404).json({ error:'User not found' });
  const added=ensureLeagueMember(league, user);
  if(added) await persistPredictions();
  res.json({ league:publicLeagueData(league, user.id) });
});

app.get('/api/predictions/leagues/:leagueId/leaderboard', async (req,res)=>{
  const leagueId=req.params.leagueId;
  const userId=resolveUserFromRequest(req);
  let league=null;
  let memberSet=null;
  let nameMap=null;
  if(leagueId && leagueId!=='global'){
    league=findLeagueById(leagueId);
    if(!league) return res.status(404).json({ error:'League not found' });
    if(!userId) return res.status(401).json({ error:'Sign in to view this league' });
    const isMember=(league.members||[]).some(member=>member.userId===userId);
    if(!isMember) return res.status(403).json({ error:'Join league to view leaderboard' });
    memberSet=new Set((league.members||[]).map(member=>member.userId));
    nameMap=new Map((league.members||[]).map(member=>[member.userId, member.name]));
  }
  const payload={};
  LEADERBOARD_WINDOWS.forEach(windowKey=>{
    const range=leaderboardRange(windowKey);
    const rows=computeLeaderboard(entry=>{
      if(memberSet && !memberSet.has(entry.userKey)) return false;
      if(range.start && (!entry.ts || entry.ts<range.start)) return false;
      return true;
    }, nameMap);
    payload[windowKey]={ rows, range };
  });
  const leaguePayload=leagueId==='global'
    ? { id:'global', name:'Global board', memberCount:null }
    : publicLeagueData(league, userId);
  res.json({ league:leaguePayload, leaderboards:payload });
});

app.get('/api/predictions/:gameId',(req,res)=>{
  const gameId=req.params.gameId;
  const list=predictionsLog.filter(entry=>entry.gameId===gameId);
  const summaries=list.map(entry=>({
    id:entry.id,
    prediction:entry.prediction,
    points:entry.points||0,
    resultEvaluated:Boolean(entry.resultEvaluated),
    author:entry.author||'Fan',
    submittedAt:entry.ts
  }));
  res.json({ predictions:summaries });
});

app.post('/api/predictions/:gameId', async (req,res)=>{
  const gameId=req.params.gameId;
  const prediction=canonicalPrediction(req.body?.prediction);
  if(!prediction) return res.status(400).json({ error:'prediction required' });
  const userKey=predictorKey(req);
  const author=sanitizeFanAuthor(req.body?.author || req.body?.name || req.body?.guestName || 'Fan');
  const entry={
    id:crypto.randomUUID(),
    gameId,
    userKey,
    author,
    prediction,
    ts:Date.now(),
    resultEvaluated:false,
    points:0
  };
  predictionsLog.push(entry);
  await persistPredictions();
  res.json({ success:true, entry });
});

app.get('/api/leaderboard',(req,res)=>{
  res.json({ leaderboard:computeLeaderboard() });
});

app.get('/api/notify/list',(req,res)=>{
  const owner=resolveOwnerKey(req);
  if(!owner) return res.json({ subscriptions:[] });
  res.json({ subscriptions:getSubscriptions(owner) });
});

app.post('/api/notify/subscribe', async (req,res)=>{
  const owner=resolveOwnerKey(req);
  if(!owner) return res.status(400).json({ error:'User or guest ID required' });
  const targetId=req.body?.targetId;
  const targetType=req.body?.targetType==='team'?'team':'game';
  if(!targetId) return res.status(400).json({ error:'targetId required' });
  const entry=upsertSubscription(owner, targetType, String(targetId));
  await persistNotifyStore();
  res.json({ subscription:entry, subscriptions:getSubscriptions(owner) });
});

app.post('/api/notify/unsubscribe', async (req,res)=>{
  const owner=resolveOwnerKey(req);
  if(!owner) return res.status(400).json({ error:'User or guest ID required' });
  const targetId=req.body?.targetId;
  const targetType=req.body?.targetType==='team'?'team':'game';
  if(!targetId) return res.status(400).json({ error:'targetId required' });
  const removed=removeSubscription(owner, targetType, String(targetId));
  if(removed) await persistNotifyStore();
  res.json({ success:true, subscriptions:getSubscriptions(owner) });
});

app.get('/api/teams/:teamId', async (req,res)=>{
  try{
    const profile=await getTeamProfile(req.params.teamId);
    if(!profile) return res.status(404).json({ error:'Team not found' });
    res.json(profile);
  }catch(err){
    console.error('Team profile error', err.message);
    res.status(502).json({ error:'Team lookup failed' });
  }
});

app.get('/api/players/:playerId', async (req,res)=>{
  try{
    const profile=await getPlayerProfile(req.params.playerId);
    if(!profile) return res.status(404).json({ error:'Player not found' });
    res.json(profile);
  }catch(err){
    console.error('Player profile error', err.message);
    res.status(502).json({ error:'Player lookup failed' });
  }
});

app.get('/api/search/players', async (req,res)=>{
  const query=(req.query.q || req.query.query || '').trim();
  if(!query) return res.status(400).json({ error:'Query required' });
  const limit=Math.max(1, Math.min(Number(req.query.limit)||10, 25));
  try{
    const results=await dataProvider.searchPlayers(query, { limit });
    res.json({ results });
  }catch(err){
    console.error('Player search error', err.message);
    res.status(502).json({ error:'Player search unavailable' });
  }
});

app.get('/api/videos/:gameId',(req,res)=>{
  res.json({ videos:getVideosForGame(req.params.gameId) });
});

app.get('/api/videos/latest',(req,res)=>{
  const limit=Math.min(Number(req.query.limit)||12,50);
  res.json({ videos:getLatestVideos(limit) });
});

app.get('/api/creators',(req,res)=>{
  res.json({
    creators:creatorProfiles.slice(0,40),
    entries:creatorEntries.slice(0,60)
  });
});

app.post('/api/creators/content', async (req,res)=>{
  const body=req.body||{};
  const creator=upsertCreatorProfile({
    creatorId:body.creatorId,
    creatorName:body.creatorName,
    name:body.creatorName,
    avatar:body.avatar,
    channel:body.channel,
    bio:body.bio
  });
  if(!creator) return res.status(400).json({ error:'creatorName or creatorId required' });
  const title=(body.title||'').toString().trim();
  const url=(body.url||'').toString().trim();
  if(!title || !url) return res.status(400).json({ error:'title and url required' });
  const entry=normalizeCreatorEntry({
    creatorId:creator.id,
    creatorName:creator.name,
    title:title.slice(0,140),
    description:(body.description||'').toString().trim().slice(0,280),
    url,
    type:body.type==='audio'?'audio':'video',
    duration:(body.duration||'').toString().slice(0,20),
    tags:Array.isArray(body.tags)?body.tags.map(tag=>String(tag).slice(0,20)).slice(0,5):[],
    thumbnail:(body.thumbnail||'').toString().trim()
  });
  creator.lastUpload=entry.ts;
  creator.submissions=(creator.submissions||0)+1;
  creatorEntries.unshift(entry);
  creatorEntries=creatorEntries.slice(0,200);
  await persistCreatorContent();
  res.json({ entry, creator });
});

app.get('/api/subscription', (req,res)=>{
  const owner=resolveOwnerKey(req);
  const record=getSubscriptionRecord(owner);
  res.json({ subscription:record || { status:'none' } });
});

app.post('/api/subscription', async (req,res)=>{
  const owner=resolveOwnerKey(req);
  if(!owner) return res.status(400).json({ error:'User or guest ID required' });
  const action=(req.body?.action||'').toLowerCase();
  if(action!=='start' && action!=='cancel'){
    return res.status(400).json({ error:'action must be start or cancel' });
  }
  if(action==='start'){
    const record={
      owner,
      status:'active',
      startedAt:Date.now(),
      plan:req.body?.plan || 'premium',
      perks:['adfree','extended_insights','unlimited_replays']
    };
    subscriptionMap.set(owner, record);
    await persistSubscriptions();
    logAnalyticsEvent('subscription:start',{ owner });
    return res.json({ subscription:record });
  }
  subscriptionMap.delete(owner);
  await persistSubscriptions();
  logAnalyticsEvent('subscription:cancel',{ owner });
  res.json({ subscription:{ status:'canceled', endedAt:Date.now() } });
});

const scheduleCache=new Map();
app.get('/api/schedule', async (req,res)=>{
  const date=req.query.date || todayStr();
  const leaguesParam=(req.query.leagues||'mlb,nba,nfl,ncaaf,ncaam,nhl,ufc').split(',').map(v=>v.trim().toLowerCase()).filter(Boolean);
  const favoritesParam=(req.query.favorites||'').split(',').map(v=>v.trim()).filter(Boolean).slice(0,50);
  const favoritesKey=favoritesParam.slice().sort().join('|');
  const favoritesSet=new Set(favoritesParam.map(String));
  const ownerKey=resolveOwnerKey(req);
  let providerKey=(dataProvider?.id || dataProvider?.name || 'espn').toLowerCase();
  let cacheKey=`${providerKey}:${date}:${leaguesParam.join(',')}:${favoritesKey}`;
  const cached=scheduleCache.get(cacheKey);
  if(cached && Date.now()-cached.ts<60000){
    const payload={ ...cached.payload };
    if(ownerKey){
      payload.alerts=evaluateReminders(ownerKey, payload.games);
      payload.reminders=getSubscriptions(ownerKey);
    }
    return res.json(payload);
  }
  const scheduleParams={ date, leagues:leaguesParam, favorites:favoritesParam };
  let games=await fetchScheduleFromProvider(dataProvider, scheduleParams);
  if(!games.length){
    console.warn(`[schedule] Provider ${dataProvider?.id||'unknown'} returned no games for ${date}. Retrying with ESPN fallback.`);
    const fallback=await providerFactories.espn();
    const fallbackGames=await fetchScheduleFromProvider(fallback, scheduleParams);
    if(fallbackGames.length){
      games=fallbackGames;
      dataProvider=fallback;
      console.warn('[schedule] Switched to ESPN provider after fallback success.');
      providerKey=(dataProvider?.id || dataProvider?.name || 'espn').toLowerCase();
      cacheKey=`${providerKey}:${date}:${leaguesParam.join(',')}:${favoritesKey}`;
    }else{
      console.warn('[schedule] Fallback provider also returned no games.');
    }
  }
  games=(games||[]).sort(sortGames);
  console.log(`[schedule] Serving ${games.length} games for ${date}.`);
  const recapsForDay=[];
  let recapsDirty=false;
  let eloDirty=false;
  games.forEach(game=>{
    game.excitementScore=computeExcitementScore(game, favoritesSet);
    evaluatePredictionsForGame(game);
    const state=(game.status?.state||'').toLowerCase();
    if(state==='post'){
      let recap=getRecapEntry(game.id);
      if(!recap){
        const text=generateRecapText(game);
        if(text){
          recap=storeRecapEntry(game,text);
          recapsDirty=true;
        }
      }
      if(recap){
        game.recap=recap.text;
        recapsForDay.push(recap);
      }
    }else{
      const recap=getRecapEntry(game.id);
      if(recap){
        game.recap=recap.text;
      }
    }
    game.highlightMoments=buildHighlightMoments(game);
    if(updateEloRatingsForGame(game)) eloDirty=true;
  });
  latestScheduleGames=games;
  latestScheduleStamp=Date.now();
  if(recapsDirty){
    await persistRecaps();
  }
  if(eloDirty){
    await persistEloStore();
  }
  const byId={};
  games.forEach(game=>{ if(game?.id) byId[game.id]=game; });
  const payload={ date, leagues:leaguesParam, games, recaps:recapsForDay.slice(0,12), provider:providerKey, byId };
  scheduleCache.set(cacheKey,{ ts:Date.now(), payload });
  if(ownerKey){
    payload.alerts=evaluateReminders(ownerKey, games);
    payload.reminders=getSubscriptions(ownerKey);
  }
  res.json(payload);
});

const newsCache=new Map();
app.get('/api/news', async (req,res)=>{
  const leagues=(req.query.leagues||'mlb,nba,nfl,ncaaf,ncaam,nhl,ufc').split(',').map(v=>v.trim().toLowerCase()).filter(Boolean);
  const key=leagues.join(',');
  const cached=newsCache.get(key);
  if(cached && Date.now()-cached.ts<120000){
    return res.json(cached.payload);
  }
  const batches=await Promise.all(leagues.map(async lg=>{
    const endpoint=NEWS_ENDPOINTS[lg];
    if(!endpoint) return [];
    try{
      const data=await fetchJSON(endpoint);
      return (data.articles||[]).map(article=>({
        id:article.id,
        headline:article.headline,
        description:article.description,
        link:article.links?.web?.href||'#',
        image:article.images?.[0]?.url||'',
        published:article.published,
        league:lg.toUpperCase()
      }));
    }catch(err){
      console.error('News fetch failed', lg, err.message);
      return [];
    }
  }));
  const merged=batches.flat().sort((a,b)=>new Date(b.published||0)-new Date(a.published||0));
  const payload={ headlines: merged.slice(0,20) };
  newsCache.set(key,{ ts:Date.now(), payload });
  res.json(payload);
});

app.get('/api/standings', async (req,res)=>{
  const leagues=(req.query.leagues||'mlb,nba,nfl,nhl').split(',').map(v=>v.trim().toLowerCase()).filter(lg=>STANDINGS_ENDPOINTS[lg]);
  if(!leagues.length) return res.json({ standings:{} });
  const entries=await Promise.all(leagues.map(async lg=>{
    const endpoint=STANDINGS_ENDPOINTS[lg];
    try{
      const data=await fetchJSON(endpoint);
      return { league:lg.toUpperCase(), rows:normalizeStandingsRows(data) };
    }catch(err){
      console.error('Standings fetch failed', lg, err.message);
      return { league:lg.toUpperCase(), rows:[] };
    }
  }));
  const standings={};
  entries.forEach(entry=>{ standings[entry.league]=entry.rows; });
  res.json({ standings });
});

app.get('/api/powerrankings',(req,res)=>{
  res.json(buildPowerRankingSnapshot());
});

const highlightCache=new Map();
app.get('/api/highlights', async (req,res)=>{
  const query=(req.query.q || req.query.query || '').trim();
  if(!query) return res.status(400).json({ error:'Query required' });
  if(!YOUTUBE_API_KEY) return res.status(503).json({ error:'Highlights disabled. Set YOUTUBE_API_KEY.' });
  const key=query.toLowerCase();
  const cached=highlightCache.get(key);
  if(cached && Date.now()-cached.ts<10*60*1000){
    return res.json(cached.payload);
  }
  try{
    const url=new URL('https://www.googleapis.com/youtube/v3/search');
    url.searchParams.set('part','snippet');
    url.searchParams.set('type','video');
    url.searchParams.set('order','date');
    url.searchParams.set('maxResults','6');
    url.searchParams.set('q',query);
    url.searchParams.set('key',YOUTUBE_API_KEY);
    const data=await fetchJSON(url.toString());
    const videos=(data.items||[]).map(item=>({
      id:item.id?.videoId,
      title:item.snippet?.title,
      description:item.snippet?.description,
      channel:item.snippet?.channelTitle,
      published:item.snippet?.publishedAt,
      thumbnail:item.snippet?.thumbnails?.medium?.url || item.snippet?.thumbnails?.default?.url,
      url:item.id?.videoId ? `https://www.youtube.com/watch?v=${item.id.videoId}` : null
    })).filter(video=>video.id && video.url);
    const payload={ query, videos };
    highlightCache.set(key,{ ts:Date.now(), payload });
    res.json(payload);
  }catch(err){
    console.error('Highlights fetch failed', err.message);
    res.status(502).json({ error:'Highlight lookup failed' });
  }
});

app.get('/api/shotchart', async (req,res)=>{
  const gameId=String(req.query.gameId||'').trim();
  const league=String(req.query.league||'').toLowerCase();
  if(!gameId) return res.status(400).json({ error:'gameId required' });
  if(league && league!=='nba'){
    return res.json({ gameId, shots:[], teams:[], players:[], periods:[], note:'Shot charts currently available for NBA games only.' });
  }
  try{
    const data=await getShotChart(gameId);
    res.json(data);
  }catch(err){
    console.error('Shot chart route failed', err.message);
    res.status(502).json({ error:'Unable to load shot plot' });
  }
});

app.get('/api/drives', async (req,res)=>{
  const gameId=String(req.query.gameId||'').trim();
  const league=String(req.query.league||'').toLowerCase();
  if(!gameId) return res.status(400).json({ error:'gameId required' });
  if(league && league!=='nfl'){
    return res.json({ gameId, drives:[], teams:[], note:'Drive charts currently support NFL games only.' });
  }
  try{
    const payload=await getDriveChart(gameId);
    res.json(payload);
  }catch(err){
    console.error('Drive chart route failed', err.message);
    res.status(502).json({ error:'Unable to load drive chart' });
  }
});

app.use(express.static(__dirname));
app.get('*',(req,res)=>{
  if(req.path.startsWith('/api/')) return res.status(404).json({ error:'Not found' });
  res.sendFile(path.join(__dirname,'index.html'));
});

const PORT = process.env.PORT || 5050;
const server = app.listen(PORT,()=>{
  console.log(`StreamDashboard API running on http://localhost:${PORT}`);
});

const wss = new WebSocketServer({ noServer:true });

wss.on('connection',(ws,request)=>{
  ws.room='global';
  ws.ip=request.socket.remoteAddress;
  socketClients.add(ws);
  pushRoomState(ws,'global');
  ws.on('message',async raw=>{
    try{
      const data=JSON.parse(raw);
      if(data.type==='join'){
        ws.room=data.gameId || 'global';
        await pushRoomState(ws, ws.room);
      }else if(data.type==='chat'){
        const clean=sanitizeMessage(data.body);
        if(!clean){
          ws.send(JSON.stringify({ type:'error', error:'Message rejected' }));
          return;
        }
        const key=ws.ip || 'socket';
        const now=Date.now();
        const last=chatRateLimiter.get(key)||0;
        if(now-last<RATE_WINDOW){
          ws.send(JSON.stringify({ type:'error', error:'Slow down' }));
          return;
        }
        chatRateLimiter.set(key, now);
        await appendChatMessage(data.gameId||ws.room||'global',{
          author:(data.author||'Guest').slice(0,18),
          body:clean,
          ts:now
        });
        logAnalyticsEvent('chat:message',{ gameId:data.gameId||ws.room||'global', author:data.author||'Guest', source:'ws', ip:ws.ip });
      }
    }catch(err){
      ws.send(JSON.stringify({ type:'error', error:'Invalid payload' }));
    }
  });
  ws.on('close',()=>socketClients.delete(ws));
});

server.on('upgrade',(request, socket, head)=>{
  if(request.url==='/chat'){
    wss.handleUpgrade(request, socket, head, ws=>{
      wss.emit('connection', ws, request);
    });
  }else{
    socket.destroy();
  }
});
