import { useEffect, useRef } from 'react'

/**
 * useInterval — setInterval that respects the latest callback reference.
 * Safe to use with closures over state.
 */
export default function useInterval(callback, delay) {
  const savedCallback = useRef(callback)

  useEffect(() => {
    savedCallback.current = callback
  }, [callback])

  useEffect(() => {
    if (delay === null) return
    const id = setInterval(() => savedCallback.current(), delay)
    return () => clearInterval(id)
  }, [delay])
}
