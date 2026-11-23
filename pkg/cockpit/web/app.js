// gracegate Cockpit Live Telemetry Engine
(function() {
  const startTime = Date.now();

  const el = {
    globalStatusPill: document.getElementById('global-status-pill'),
    globalStatusText: document.getElementById('global-status-text'),
    btnTriggerDrain: document.getElementById('btn-trigger-drain'),
    counterInFlight: document.getElementById('counter-inflight'),
    badgeZeroLoss: document.getElementById('badge-zero-loss'),
    badgeUptime: document.getElementById('badge-uptime'),
    valTotalReqs: document.getElementById('val-total-reqs'),
    valTotalErrs: document.getElementById('val-total-errs'),
    valActiveClients: document.getElementById('val-active-clients'),
    valAvgLatency: document.getElementById('val-avg-latency'),
    protoHttp1Val: document.getElementById('proto-http1-val'),
    protoHttp2Val: document.getElementById('proto-http2-val'),
    protoGrpcVal: document.getElementById('proto-grpc-val'),
    timelineContainer: document.getElementById('timeline-container'),
    timelineEmptyMsg: document.getElementById('timeline-empty-msg'),
    timelinePhaseBadge: document.getElementById('timeline-phase-badge'),
    propLag: document.getElementById('prop-lag'),
    propQuorum: document.getElementById('prop-quorum'),
    propInflightDur: document.getElementById('prop-inflight-dur'),
    propTotalTime: document.getElementById('prop-total-time'),
    btnToggleSim: document.getElementById('btn-toggle-sim'),
    btnSimText: document.getElementById('btn-sim-text'),
    selectConcurrency: document.getElementById('select-concurrency'),
    simStatsBar: document.getElementById('sim-stats-bar'),
    simSentVal: document.getElementById('sim-sent-val'),
    simSuccessVal: document.getElementById('sim-success-val'),
    simStatusBadge: document.getElementById('sim-status-badge')
  };

  // Uptime ticker
  setInterval(updateUptime, 1000);

  function updateUptime() {
    const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
    const hrs = String(Math.floor(elapsedSec / 3600)).padStart(2, '0');
    const mins = String(Math.floor((elapsedSec % 3600) / 60)).padStart(2, '0');
    const secs = String(elapsedSec % 60).padStart(2, '0');
    if (el.badgeUptime) {
      el.badgeUptime.textContent = `UPTIME: ${hrs}:${mins}:${secs}`;
    }
  }

  // Update UI with incoming telemetry data
  function renderTelemetry(data) {
    if (!data) return;

    const snap = data.metrics || data;
    const phase = data.phase || 'RUNNING';

    // Global phase pill
    if (el.globalStatusText) {
      el.globalStatusText.textContent = phase;
    }
    if (el.globalStatusPill) {
      el.globalStatusPill.className = 'live-status-pill';
      if (phase.includes('DRAINING')) {
        el.globalStatusPill.classList.add('draining');
      } else if (phase === 'TERMINATED') {
        el.globalStatusPill.classList.add('terminated');
      }
    }

    if (el.timelinePhaseBadge) {
      el.timelinePhaseBadge.textContent = `PHASE: ${phase}`;
    }

    // Counters
    const inFlight = snap.in_flight_total !== undefined ? snap.in_flight_total : 0;
    if (el.counterInFlight) {
      el.counterInFlight.textContent = inFlight;
    }

    if (el.valTotalReqs) {
      el.valTotalReqs.textContent = (snap.total_requests || 0).toLocaleString();
    }
    if (el.valTotalErrs) {
      el.valTotalErrs.textContent = (snap.total_errors || 0).toLocaleString();
    }
    if (el.valActiveClients) {
      el.valActiveClients.textContent = (snap.active_clients || 0).toLocaleString();
    }

    if (el.valAvgLatency && snap.avg_latency_ns) {
      const ms = (snap.avg_latency_ns / 1e6).toFixed(2);
      el.valAvgLatency.textContent = `${ms} ms`;
    }

    // Protocol breakdown
    if (el.protoHttp1Val) {
      el.protoHttp1Val.textContent = snap.in_flight_http1 || 0;
    }
    if (el.protoHttp2Val) {
      el.protoHttp2Val.textContent = snap.in_flight_http2 || 0;
    }
    if (el.protoGrpcVal) {
      el.protoGrpcVal.textContent = snap.in_flight_grpc || 0;
    }

    // Milestones & Waterfall
    const milestones = data.milestones || [];
    if (milestones.length > 0) {
      if (el.timelineEmptyMsg) {
        el.timelineEmptyMsg.style.display = 'none';
      }
      renderMilestones(milestones);
    }

    // Propagation stats
    if (data.timeline) {
      const tl = data.timeline;
      if (el.propLag) el.propLag.textContent = `${tl.measured_ingress_delay_ms || 0} ms`;
      if (el.propQuorum) el.propQuorum.textContent = `${(tl.quorum_duration_ms || 3000).toLocaleString()} ms`;
      if (el.propInflightDur) el.propInflightDur.textContent = `${tl.inflight_drain_ms || 0} ms`;
      if (el.propTotalTime) el.propTotalTime.textContent = `${tl.total_drain_ms || 0} ms`;
    }

    // Simulation stats
    if (data.simulation) {
      const sim = data.simulation;
      if (sim.active) {
        if (el.simStatsBar) el.simStatsBar.style.display = 'flex';
        if (el.simSentVal) el.simSentVal.textContent = sim.requests_sent.toLocaleString();
        if (el.simSuccessVal) el.simSuccessVal.textContent = sim.requests_success.toLocaleString();
        if (el.btnSimText) el.btnSimText.textContent = 'Stop Load Generator';
        if (el.btnToggleSim) el.btnToggleSim.classList.add('sim-running');
      } else {
        if (el.btnSimText) el.btnSimText.textContent = 'Start Synthetic Load';
        if (el.btnToggleSim) el.btnToggleSim.classList.remove('sim-running');
      }
    }
  }

  function renderMilestones(milestones) {
    if (!el.timelineContainer) return;

    // Retain only valid rows
    el.timelineContainer.innerHTML = '';
    milestones.forEach(m => {
      const row = document.createElement('div');
      row.className = 'milestone-row';
      if (m.name.includes('DRAIN')) {
        row.classList.add('milestone-drain');
      } else if (m.name.includes('TERMINATED')) {
        row.classList.add('milestone-term');
      }

      const offsetFormatted = `+${m.offset_ms || 0} ms`;

      row.innerHTML = `
        <span class="milestone-time">${offsetFormatted}</span>
        <span class="milestone-name">${m.name}</span>
        <span class="milestone-desc">${m.description}</span>
      `;
      el.timelineContainer.appendChild(row);
    });
  }

  // Trigger Drain action
  if (el.btnTriggerDrain) {
    el.btnTriggerDrain.addEventListener('click', async () => {
      if (confirm('Trigger graceful pod draining sequence now? This will switch readiness to 503 and flush in-flight requests.')) {
        try {
          el.btnTriggerDrain.disabled = true;
          el.btnTriggerDrain.innerHTML = '<span class="pulse-indicator"></span> Draining...';
          const res = await fetch('/api/drain', { method: 'POST' });
          if (!res.ok) {
            alert('Failed to trigger drain: ' + res.statusText);
            el.btnTriggerDrain.disabled = false;
            el.btnTriggerDrain.innerHTML = '<span class="btn-icon">⚡</span> <span class="btn-label">Trigger Draining</span>';
          }
        } catch (err) {
          alert('Network error triggering drain: ' + err.message);
          el.btnTriggerDrain.disabled = false;
        }
      }
    });
  }

  // Toggle Traffic Simulator action
  let isSimRunning = false;
  if (el.btnToggleSim) {
    el.btnToggleSim.addEventListener('click', async () => {
      const concurrency = parseInt(el.selectConcurrency ? el.selectConcurrency.value : '35', 10);
      const action = isSimRunning ? 'stop' : 'start';

      try {
        el.btnToggleSim.disabled = true;
        const res = await fetch('/api/simulate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: action, concurrency: concurrency })
        });

        if (res.ok) {
          isSimRunning = !isSimRunning;
          if (isSimRunning) {
            if (el.simStatsBar) el.simStatsBar.style.display = 'flex';
            if (el.btnSimText) el.btnSimText.textContent = 'Stop Load Generator';
            el.btnToggleSim.classList.add('sim-running');
          } else {
            if (el.btnSimText) el.btnSimText.textContent = 'Start Synthetic Load';
            el.btnToggleSim.classList.remove('sim-running');
          }
        }
      } catch (err) {
        alert('Simulator error: ' + err.message);
      } finally {
        el.btnToggleSim.disabled = false;
      }
    });
  }

  // SSE connection with auto-reconnect and polling fallback
  let eventSource = null;

  function initSSE() {
    try {
      eventSource = new EventSource('/api/events');
      eventSource.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          renderTelemetry(payload);
        } catch (e) {
          // ignore parse error
        }
      };

      eventSource.onerror = () => {
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }
        // Fallback to polling
        pollStatus();
        setTimeout(initSSE, 5000);
      };
    } catch (e) {
      pollStatus();
    }
  }

  async function pollStatus() {
    try {
      const res = await fetch('/api/status');
      if (res.ok) {
        const data = await res.json();
        renderTelemetry(data);
      }
    } catch (e) {
      // offline
    }
  }

  // Start telemetry loop
  initSSE();
  setInterval(pollStatus, 1000);
})();
