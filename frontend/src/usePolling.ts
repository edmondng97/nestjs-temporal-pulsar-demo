import { useEffect, useRef, useState } from 'react';

// Poll fn every ms. Keeps last good data; surfaces the latest error separately
// so a transient API failure doesn't blank the UI.
export function usePolling<T>(fn: () => Promise<T>, ms: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const d = await fnRef.current();
        if (alive) { setData(d); setError(null); }
      } catch (e) {
        if (alive) setError((e as Error).message);
      }
    };
    void tick();
    const t = setInterval(tick, ms);
    return () => { alive = false; clearInterval(t); };
  }, [ms]);

  return { data, error };
}
