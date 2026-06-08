const DATASINK_URL = 'https://europe-west3-faastreams.cloudfunctions.net/datasink-fetcher';

const HAZARD_ZONES = [
  { name: 'Subsea Pipeline Corridor Kattegat', coords: [[57.0, 10.0], [57.0, 10.5], [57.5, 10.5], [57.5, 10.0]], color: '#49754E' },
  { name: 'Offshore Wind Exclusion North Sea', coords: [[55.5, 7.5], [55.5, 8.0], [56.0, 8.0], [56.0, 7.5]], color: '#6DAA7D' },
  { name: 'Restricted Zone Skagerrak', coords: [[58.0, 10.5], [58.0, 11.0], [58.5, 11.0], [58.5, 10.5]], color: '#74B57B' },
  { name: 'Military Exercise Area Bornholm', coords: [[55.5, 15.5], [55.5, 16.0], [56.0, 16.0], [56.0, 15.5]], color: '#49754E' },
];

let map, markersLayer;

function initMap() {
  map = L.map('map').setView([56.0, 11.0], 6);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap contributors', maxZoom: 18,
  }).addTo(map);
  markersLayer = L.layerGroup().addTo(map);
  HAZARD_ZONES.forEach(function(zone) {
    L.polygon(zone.coords, {
      color: zone.color, weight: 2, opacity: 0.9, fillOpacity: 0.14, dashArray: '6, 6'
    }).bindPopup('<strong>' + zone.name + '</strong>').addTo(map);
  });
  setTimeout(function() { map.invalidateSize(); }, 200);
}

function startClock() {
  function tick() {
    var now = new Date();
    var h = String(now.getHours()).padStart(2, '0');
    var m = String(now.getMinutes()).padStart(2, '0');
    var s = String(now.getSeconds()).padStart(2, '0');
    var clockEl = document.getElementById('clock');
    if (clockEl) clockEl.textContent = h + ':' + m + ':' + s;
    var dateEl = document.getElementById('clock-date');
    if (dateEl) dateEl.textContent = now.toLocaleDateString('en-GB', {
      weekday: 'short', year: 'numeric', month: 'short', day: 'numeric'
    });
  }
  tick();
  setInterval(tick, 1000);
}

function extractPositions(entry) {
  if (entry.results && entry.results[0] && entry.results[0].positions && entry.results[0].positions.length > 0) {
    return entry.results[0].positions;
  }
  if (entry.results && entry.results.length > 0 && entry.results[0].latitude != null) {
    return entry.results;
  }
  return [];
}

function getQueryType(entry) {
  var q = (entry.query || '').toLowerCase();
  if (q.indexOf('count') !== -1) return 'COUNT';
  if (q.indexOf('select') !== -1) return 'POSITIONS';
  return 'OTHER';
}

async function loadVesselData() {
  try {
    var res = await fetch(DATASINK_URL);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    var json = await res.json();

    var allData = json.data || [];
    var faasEntries = allData.filter(function(d) { return d.pipeline === 'faas'; });
    var faasWithPositions = faasEntries.filter(function(d) { return extractPositions(d).length > 0; });
    var countQueries = faasEntries.filter(function(d) { return getQueryType(d) === 'COUNT'; });

    // Update info box stats
    var el = function(id) { return document.getElementById(id); };
    if (el('sink-count'))          el('sink-count').textContent = allData.length;
    if (el('faas-entry-count'))    el('faas-entry-count').textContent = faasEntries.length;
    if (el('count-query-count'))   el('count-query-count').textContent = countQueries.length;
    if (el('position-query-count')) el('position-query-count').textContent = faasWithPositions.length;

    faasWithPositions.sort(function(a, b) { return b.window_end - a.window_end; });

    if (!faasWithPositions.length) {
      if (el('faas-count')) el('faas-count').textContent = 'FaaS: no position data';
      return;
    }

    var latest = faasWithPositions[0];
    var positions = extractPositions(latest);
    var vesselCount = positions.length;
    if (latest.results && latest.results[0] && latest.results[0].vessel_count) {
      vesselCount = latest.results[0].vessel_count;
    }

    // Format window timestamps
    var winStart = new Date(latest.window_start * 1000).toLocaleTimeString('en-GB');
    var winEnd   = new Date(latest.window_end   * 1000).toLocaleTimeString('en-GB');
    var queryLabel = getQueryType(latest);

    if (el('faas-count')) el('faas-count').textContent = 'FaaS: ' + vesselCount + ' vessels';

    var subheader = el('results-subheader');
    if (subheader) {
      subheader.textContent = 'Window ' + winStart + ' – ' + winEnd + ' · query: ' + queryLabel + ' · showing ' + positions.length + ' raw records';
    }

    markersLayer.clearLayers();
    var seen = {};
    var uniqueVessels = [];

    positions.forEach(function(v) {
      if (!v.latitude || !v.longitude) return;
      if (v.latitude === 91.0) return;
      if (seen[v.mmsi]) return;
      seen[v.mmsi] = true;
      uniqueVessels.push(v);

      L.circleMarker([v.latitude, v.longitude], {
        radius: 6,
        fillColor: '#378ADD',
        color: '#B5D4F4',
        weight: 1.5,
        fillOpacity: 0.85,
      }).bindPopup(
        '<strong>' + (v.name || 'Unknown vessel') + '</strong><br>' +
        'MMSI: ' + (v.mmsi || '-') + '<br>' +
        'Status: ' + (v.navigationalStatus || '-') + '<br>' +
        'Speed: ' + (v.sog != null ? v.sog : '-') + ' kn'
      ).addTo(markersLayer);
    });

    var list = el('results-list');
    if (!uniqueVessels.length) {
      list.innerHTML = '<p class="empty">No vessels found.</p>';
    } else {
      list.innerHTML = uniqueVessels.map(function(v) {
        return '<div class="result-item">' +
          '<div class="result-item-title">' + (v.name || 'Unknown Vessel') + '</div>' +
          '<div class="result-meta">' +
          '<span>' + (v.mmsi || '-') + '</span>' +
          '<span>' + (v.sog != null ? v.sog : '-') + ' kn</span>' +
          '</div></div>';
      }).join('');
    }

    if (el('count-badge')) el('count-badge').textContent = uniqueVessels.length;

  } catch (e) {
    console.error(e);
    var fc = document.getElementById('faas-count');
    if (fc) fc.textContent = 'FaaS: error';
  }
}

async function boot() {
  initMap();
  startClock();
  await loadVesselData();
  setInterval(loadVesselData, 30000);
}

document.addEventListener('DOMContentLoaded', boot);