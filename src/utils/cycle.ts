/**
 * Creates a cyclical counter that wraps around from start to stop
 */
export function cycle(stop: number, start: number = 0) {
  let val = start;
  return {
    current: () => val,
    next: () => {
      const current = val;
      val = val >= stop ? start : val + 1;
      return current;
    },
    reset: () => {
      val = start;
    },
  };
}
