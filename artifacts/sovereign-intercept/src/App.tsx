import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { QueryClient, QueryClientProvider, useQueryClient } from '@tanstack/react-query';
import { Activity, AlertTriangle, Check, CircleDot, Clock3, RotateCcw, Send, Shield, ShieldAlert, Terminal, Wifi, WifiOff } from 'lucide-react';
import { useHealthCheck, getHealthCheckQueryKey, useIngestEvent, useGetEvents, getGetEventsQueryKey, useGetSystemStatus, getGetSystemStatusQueryKey, useResetQuarantine } from '@workspace/api-client-react';
import type { IngestInput, SystemStatus, TelemetryEvent } from '@workspace/api-client-react';
import { ErrorBoundary } from '@/components/error-boundary';
import { Toaster } from '@/components/ui/toaster';
import { TooltipProvider } from '@/components/ui/tooltip';
import NotFound from '@/pages/not-found';
import { Route, Switch, useLocation, Router as WouterRouter } from 'wouter';

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: true } },
});

type PendingItem = { localId: string; queuedAt: string; input: IngestInput };
type Snapshot = { events: TelemetryEvent[]; status: SystemStatus; savedAt: string };
const QUEUE_KEY = 'sovereign-intercept.pending.v1';
const SNAPSHOT_KEY = 'sovereign-intercept.snapshot.v1';

const safeRead = <T,>(key: string, fallback: T): T => {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
};

const writeStorage = (key: string, value: unknown) => {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* storage may be unavailable */ }
};

function useOfflineQueue() {
  const [queue, setQueue] = useState<PendingItem[]>(() => safeRead(QUEUE_KEY, []));
  const queueRef = useRef(queue);
  queueRef.current = queue;
  const update = useCallback((next: PendingItem[]) => {
    queueRef.current = next;
    setQueue(next);
    writeStorage(QUEUE_KEY, next);
  }, []);
  return { queue, queueRef, update };
}

function Home() {
  const queryClient = useQueryClient();
  const health = useHealthCheck({ query: { queryKey: getHealthCheckQueryKey(), refetchInterval: 7000, retry: 0 } });
  const statusQuery = useGetSystemStatus({ query: { queryKey: getGetSystemStatusQueryKey(), refetchInterval: 7000, retry: 1 } });
  const eventsQuery = useGetEvents({ limit: 40 }, { query: { queryKey: getGetEventsQueryKey({ limit: 40 }), refetchInterval: 7000, retry: 1 } });
  const ingest = useIngestEvent();
  const ingestAsyncRef = useRef(ingest.mutateAsync);
  ingestAsyncRef.current = ingest.mutateAsync;
  const reset = useResetQuarantine();
  const { queue, queueRef, update } = useOfflineQueue();
  const [snapshot, setSnapshot] = useState<Snapshot | null>(() => safeRead<Snapshot | null>(SNAPSHOT_KEY, null));
  const [eventType, setEventType] = useState<IngestInput['event_type']>('system_log');
  const [source, setSource] = useState('edge-gateway-03');
  const [payloadText, setPayloadText] = useState(JSON.stringify({
    level: 'INFO',
    message: 'heartbeat received',
    host: 'edge-gateway-03',
    timestamp: new Date().toISOString(),
  }, null, 2));
  const [preset, setPreset] = useState<'normal' | 'anomalous' | 'custom'>('normal');
  const [formError, setFormError] = useState('');
  const [actionError, setActionError] = useState('');
  const retryLock = useRef(false);

  const connected = health.isSuccess && !health.isError;
  const status = connected ? statusQuery.data ?? snapshot?.status ?? null : snapshot?.status ?? null;
  const events = connected ? eventsQuery.data ?? snapshot?.events ?? [] : snapshot?.events ?? [];
  const isOffline = !connected;
  const snapshotTime = snapshot?.savedAt ? new Date(snapshot.savedAt) : null;

  useEffect(() => {
    if (connected && statusQuery.data && eventsQuery.data) {
      const next = { status: statusQuery.data, events: eventsQuery.data, savedAt: new Date().toISOString() };
      setSnapshot(next);
      writeStorage(SNAPSHOT_KEY, next);
    }
  }, [connected, statusQuery.data, eventsQuery.data]);

  useEffect(() => {
    if (!connected || queue.length === 0 || retryLock.current) return;
    retryLock.current = true;
    const drain = async () => {
      try {
        while (queueRef.current.length > 0) {
          const item = queueRef.current[0];
          try {
            await ingestAsyncRef.current({ data: item.input });
            update(queueRef.current.filter((entry) => entry.localId !== item.localId));
            queryClient.invalidateQueries({ queryKey: getGetEventsQueryKey({ limit: 40 }) });
            queryClient.invalidateQueries({ queryKey: getGetSystemStatusQueryKey() });
          } catch {
            break;
          }
        }
      } finally {
        retryLock.current = false;
      }
    };
    void drain();
  }, [connected, health.dataUpdatedAt, queue.length, queryClient, queueRef, update]);

  const refresh = () => {
    void health.refetch();
    void statusQuery.refetch();
    void eventsQuery.refetch();
  };

  const applyPreset = (kind: 'normal' | 'anomalous') => {
    setPreset(kind);
    setFormError('');
    if (kind === 'normal') {
      setEventType('system_log');
      setSource('edge-gateway-03');
      setPayloadText(JSON.stringify({ level: 'INFO', message: 'heartbeat received', host: 'edge-gateway-03', timestamp: new Date().toISOString() }, null, 2));
    } else {
      setSource('db-primary-01');
      setEventType('database_transaction');
      setPayloadText(JSON.stringify({ user_id: 'svc-legacy', operation: 'UPDATE', table: 'accounts', row_count: 18427, authorized: false, state_change: 'privilege escalation' }, null, 2));
    }
  };

  const submitEvent = async () => {
    setFormError('');
    setActionError('');
    let payload: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(payloadText);
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') throw new Error('Payload must be a JSON object.');
      payload = parsed as Record<string, unknown>;
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Payload is not valid JSON.');
      return;
    }
    if (!source.trim()) {
      setFormError('A source identifier is required.');
      return;
    }
    const input: IngestInput = { event_type: eventType, source: source.trim(), payload };
    try {
      await ingest.mutateAsync({ data: input });
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: getGetEventsQueryKey({ limit: 40 }) }),
        queryClient.invalidateQueries({ queryKey: getGetSystemStatusQueryKey() }),
      ]);
    } catch {
      const item: PendingItem = { localId: `${Date.now()}-${Math.random().toString(16).slice(2)}`, queuedAt: new Date().toISOString(), input };
      update([...queueRef.current, item]);
      setActionError('API did not confirm receipt. Event saved locally and remains pending; no server result is shown.');
    }
  };

  const resetQuarantine = async () => {
    setActionError('');
    try {
      const result = await reset.mutateAsync();
      queryClient.setQueryData(getGetSystemStatusQueryKey(), result);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: getGetSystemStatusQueryKey() }),
        queryClient.invalidateQueries({ queryKey: getGetEventsQueryKey({ limit: 40 }) }),
      ]);
    } catch {
      setActionError('Quarantine reset was not confirmed by the API. The displayed state has not been changed.');
    }
  };

  const formattedEvents = useMemo(() => events, [events]);
  const showLoading = !snapshot && (statusQuery.isLoading || eventsQuery.isLoading);
  const showFeedError = connected && eventsQuery.isError && !snapshot;
  const stateQuarantined = status?.state === 'quarantined';
  const lastEvent = status?.last_event_at ? formatTime(status.last_event_at) : '—';

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark"><Shield size={16} strokeWidth={1.8} /></div>
          <div>
            <div className="brand-name">Sovereign Intercept Layer</div>
            <div className="brand-sub">Telemetry control plane / node 01</div>
          </div>
        </div>
        <div className="top-meta">
          <div className="api-indicator" data-testid="status-api-connectivity">
            {isOffline ? <WifiOff size={13} /> : <Wifi size={13} />}
            <span className={`api-dot ${isOffline ? 'offline' : ''}`} />
            {isOffline ? 'API OFFLINE' : 'API CONNECTED'}
          </div>
          <div className="live-tag">POLL 07S</div>
        </div>
      </header>

      <div className="dashboard">
        <div className="page-heading">
          <div>
            <div className="eyebrow">Operator console / inbound telemetry</div>
            <h1>Intercept overview</h1>
            <p className="subtitle">Inspect incoming system events. Drift beyond threshold triggers quarantine.</p>
          </div>
          {isOffline && (
            <div className="snapshot-note" data-testid="status-offline-snapshot">
              <WifiOff size={12} style={{ verticalAlign: 'middle', marginRight: 6 }} />
              {snapshotTime ? `Offline snapshot · ${formatTime(snapshotTime.toISOString())}` : 'Offline · awaiting API connection'}
            </div>
          )}
        </div>

        <section className="status-banner" aria-label="System telemetry summary">
          <div className={`state-block ${stateQuarantined ? 'is-quarantined' : ''}`}>
            <div className={`state-title ${stateQuarantined ? 'quarantined' : ''}`} data-testid="status-system-state">
              {stateQuarantined ? <ShieldAlert size={15} /> : <CircleDot size={14} />}
              {status ? (stateQuarantined ? 'QUARANTINED' : 'SOVEREIGN ACTIVE') : 'STATUS UNAVAILABLE'}
            </div>
            <div className="state-caption">{status ? (stateQuarantined ? 'Interception lock engaged' : 'Drift monitor armed') : 'No verified server status'}</div>
          </div>
          <Metric label="Events observed" value={status ? status.event_count.toLocaleString() : '—'} testId="value-event-total" />
          <Metric label="Intercepted" value={status ? status.intercepted_count.toLocaleString() : '—'} alert={!!status && status.intercepted_count > 0} testId="value-intercept-total" />
          <Metric label="Drift threshold" value={status ? formatNumber(status.threshold) : '—'} unit={status ? '%' : undefined} testId="value-drift-threshold" />
          <Metric label="Avg latency" value={status ? formatNumber(status.avg_latency_ms) : '—'} unit={status ? 'ms' : undefined} testId="value-latency" />
        </section>

        {stateQuarantined && (
          <div className="toolbar" style={{ marginTop: -4 }}>
            <div className="section-label" style={{ color: '#ff8580' }}><AlertTriangle size={14} /> Quarantine lock active</div>
            <button className="text-button danger" data-testid="button-reset-quarantine" disabled={!connected || reset.isPending} onClick={() => void resetQuarantine()}>
              <RotateCcw size={13} /> {reset.isPending ? 'RESETTING…' : 'RESET QUARANTINE'}
            </button>
          </div>
        )}

        {actionError && <div className="error-strip" role="alert" data-testid="status-action-error"><span>{actionError}</span><button className="text-button" onClick={() => setActionError('')} data-testid="button-dismiss-error">Dismiss</button></div>}

        <div className="toolbar">
          <div className="section-label"><Activity size={14} /> Live telemetry</div>
          <div className="toolbar-right">
            <span>{connected ? `LAST EVENT ${lastEvent}` : 'READING LAST VERIFIED SNAPSHOT'}</span>
            <button className="text-button" data-testid="button-refresh" onClick={refresh}><RotateCcw size={12} /> Refresh</button>
          </div>
        </div>

        <div className="layout-grid">
          <section className="panel" aria-label="Incoming event feed">
            <div className="panel-header">
              <div className="panel-heading">Incoming event feed</div>
              <div className="feed-count" data-testid="text-feed-count">{events.length} / 40 records</div>
            </div>
            {showFeedError && <div className="error-strip" role="alert"><span>Could not load events from the API.</span><button className="text-button" onClick={() => void eventsQuery.refetch()} data-testid="button-retry-events">Retry</button></div>}
            <div className="feed-table-head"><span>Timestamp</span><span>Event / source / payload</span><span style={{ textAlign: 'right' }}>Intercept</span></div>
            <div className="event-list" data-testid="list-event-feed">
              {showLoading ? (
                <><div className="skeleton" /><div className="skeleton" /><div className="skeleton" /></>
              ) : formattedEvents.length > 0 ? formattedEvents.map((event) => (
                <EventRow key={event.id} event={event} />
              )) : (
                <div className="empty-state" data-testid="state-empty-feed">
                  <div><strong>{isOffline ? 'No cached event records' : 'No events observed'}</strong><p>{isOffline ? 'The API is unavailable and this device has no successful event snapshot yet.' : 'Incoming telemetry will appear here as soon as the first event is received.'}</p></div>
                </div>
              )}
            </div>
          </section>

          <aside>
            <section className="panel">
              <div className="panel-header">
                <div className="panel-heading">Event simulator</div>
                <div className="panel-hint">MANUAL INGEST</div>
              </div>
              <div className="simulator">
                <div className="preset-row" role="group" aria-label="Event presets">
                  <button className={`preset normal ${preset === 'normal' ? 'selected' : ''}`} onClick={() => applyPreset('normal')} data-testid="button-preset-normal">
                    <strong><Check size={12} /> Normal event</strong><span>Low drift · expected telemetry</span>
                  </button>
                  <button className={`preset anomalous ${preset === 'anomalous' ? 'selected' : ''}`} onClick={() => applyPreset('anomalous')} data-testid="button-preset-anomalous">
                    <strong><AlertTriangle size={12} /> Anomalous event</strong><span>High drift · quarantine test</span>
                  </button>
                </div>
                <div className="field-row">
                  <div>
                    <label className="sim-label" htmlFor="event-type">Event type</label>
                    <select id="event-type" className="field select-field" value={eventType} onChange={(event) => { setEventType(event.target.value as IngestInput['event_type']); setPreset('custom'); }} data-testid="input-event-type">
                      <option value="system_log">system_log</option>
                      <option value="user_input">user_input</option>
                      <option value="database_transaction">database_transaction</option>
                    </select>
                  </div>
                  <div>
                    <label className="sim-label" htmlFor="event-source">Source identifier</label>
                    <input id="event-source" className="field" value={source} onChange={(event) => { setSource(event.target.value); setPreset('custom'); }} maxLength={120} placeholder="service / node" data-testid="input-event-source" />
                  </div>
                  <div>
                    <label className="sim-label" htmlFor="event-payload">JSON payload</label>
                    <textarea id="event-payload" className="field json-editor" spellCheck={false} value={payloadText} onChange={(event) => { setPayloadText(event.target.value); setPreset('custom'); }} data-testid="input-event-payload" />
                    {formError && <div className="form-error" role="alert" data-testid="text-payload-error">{formError}</div>}
                  </div>
                </div>
                <div className="send-row">
                  <span className="send-note">{connected ? 'Submit to the live ingest endpoint.' : 'API unavailable. Submit will be held locally until retry.'}</span>
                  <button className="text-button primary" onClick={() => void submitEvent()} disabled={ingest.isPending} data-testid="button-send-event">
                    <Send size={12} /> {ingest.isPending ? 'SENDING…' : 'SEND EVENT'}
                  </button>
                </div>
              </div>
            </section>

            <section className="panel queue-panel" aria-label="Pending event queue">
              <div className="panel-header">
                <div className="panel-heading">Pending queue</div>
              <div className="queue-state" data-testid="status-queue-count">{queue.length} {queue.length === 1 ? 'HELD' : 'HELD'}</div>
              </div>
              {queue.length ? (
                <div className="queue-list" data-testid="list-pending-queue">
                  {queue.map((item) => (
                    <div className="queue-item" key={item.localId} data-testid={`queue-event-${item.localId}`}>
                      <div><div className="queue-item-title">{item.input.event_type} · {item.input.source}</div><div className="queue-item-sub">Queued {formatTime(item.queuedAt)} · awaiting API confirmation</div></div>
                      <span className="queue-state">PENDING</span>
                    </div>
                  ))}
                </div>
              ) : <div className="queue-empty" data-testid="state-empty-queue">{connected ? 'No held submissions. New events are sent directly to the API.' : 'No held submissions. Events created offline will remain here until confirmed.'}</div>}
            </section>
          </aside>
        </div>
        <div className="footer-line">
          <span><Terminal size={11} style={{ verticalAlign: 'middle', marginRight: 6 }} /> SOVEREIGN INTERCEPT / CONTROL SURFACE</span>
          <span><Clock3 size={11} style={{ verticalAlign: 'middle', marginRight: 6 }} /> {status?.last_event_at ? `LAST SIGNAL ${lastEvent}` : 'LAST SIGNAL —'}</span>
        </div>
      </div>
    </main>
  );
}

function Metric({ label, value, unit, alert, testId }: { label: string; value: string; unit?: string; alert?: boolean; testId: string }) {
  return <div className="metric"><div className="metric-label">{label}</div><div className={`metric-value ${alert ? 'alert' : ''}`} data-testid={testId}>{value}{unit && <small>{unit}</small>}</div></div>;
}

function EventRow({ event }: { event: TelemetryEvent }) {
  return (
    <article className="event-row" data-testid={`row-event-${event.id}`}>
      <time className="event-time" dateTime={event.created_at}>{formatTime(event.created_at)}</time>
      <div className="event-main">
        <div className="event-meta"><span className="event-type">{event.event_type}</span><span className="event-source">{event.source}</span></div>
        <div className="event-payload">{truncate(JSON.stringify(event.payload), 220)}</div>
      </div>
      {event.quarantined ? <div className="event-reason" data-testid={`status-intercept-${event.id}`}><span className="event-status"><ShieldAlert size={11} /> INTERCEPTED</span><br />{event.intercept_reason || 'Drift threshold exceeded'}<br />DRIFT {formatNumber(event.drift_score)}</div> : <div className="event-normal" data-testid={`status-intercept-${event.id}`}><span className="event-status"><Check size={11} /> PASS</span><br />DRIFT {formatNumber(event.drift_score)}</div>}
    </article>
  );
}

function formatTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(date);
}
function formatNumber(value: number) {
  return Number.isFinite(value) ? new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value) : '—';
}
function truncate(value: string, length: number) {
  if (value.length <= length) return value;
  return `${value.slice(0, length)}…`;
}

function Router() {
  return (
    <RoutedErrorBoundary>
      <Switch>
        <Route path="/" component={Home} />
        <Route component={NotFound} />
      </Switch>
    </RoutedErrorBoundary>
  );
}

function RoutedErrorBoundary({ children }: { children: ReactNode }) {
  const [location] = useLocation();
  return <ErrorBoundary resetKey={location}>{children}</ErrorBoundary>;
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <WouterRouter base={import.meta.env.BASE_URL.replace(/\/$/, '')}>
          <Router />
        </WouterRouter>
        <Toaster />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
