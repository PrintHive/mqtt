/** Topic filters and matching, per MQTT 3.1.1 section 4.7. */

/** A filter is valid when `#` appears only as the whole last level and `+` only as a whole level. */
export const isValidFilter = (filter: string): boolean => {
  if (filter.length === 0) return false

  const levels = filter.split("/")

  return levels.every(
    (level, i) =>
      (level === "#" && i === levels.length - 1) || level === "+" || (!level.includes("#") && !level.includes("+"))
  )
}

/** A topic name is valid when it is non-empty and has no wildcards. */
export const isValidTopic = (topic: string): boolean => topic.length > 0 && !topic.includes("#") && !topic.includes("+")

interface Node<A> {
  readonly children: Map<string, Node<A>>
  /** Subscribers whose filter ends at this level. */
  readonly exact: Set<A>
  /** Subscribers whose filter ends with `#` after this level. */
  readonly rest: Set<A>
}

const node = <A>(): Node<A> => ({ children: new Map(), exact: new Set(), rest: new Set() })

/**
 * Routes topic names to the subscribers of every filter that matches them.
 *
 * Matching walks one trie level per topic level, so its cost depends on topic depth and the wildcards in play,
 * not on the number of subscriptions.
 */
export class TopicTree<A> {
  private readonly root: Node<A> = node()

  add(filter: string, subscriber: A): void {
    let at = this.root

    for (const level of filter.split("/")) {
      if (level === "#") {
        at.rest.add(subscriber)

        return
      }

      let next = at.children.get(level)

      if (next === undefined) {
        next = node()
        at.children.set(level, next)
      }

      at = next
    }

    at.exact.add(subscriber)
  }

  remove(filter: string, subscriber: A): void {
    const path: Array<Node<A>> = [this.root]
    const levels = filter.split("/")

    for (const level of levels) {
      if (level === "#") break

      const next = path.at(-1)?.children.get(level)

      if (next === undefined) return

      path.push(next)
    }

    const last = path.at(-1)

    if (last === undefined) return

    if (levels.at(-1) === "#") last.rest.delete(subscriber)
    else last.exact.delete(subscriber)

    for (let i = path.length - 1; i > 0; i--) {
      const at = path[i]
      const parent = path[i - 1]

      if (at === undefined || parent === undefined || at.children.size > 0 || at.exact.size > 0 || at.rest.size > 0) return

      parent.children.delete(levels[i - 1] ?? "")
    }
  }

  /** Calls `f` once per matching subscription. A subscriber with two matching filters is called twice. */
  match(topic: string, f: (subscriber: A) => void): void {
    const levels = topic.split("/")

    // Section 4.7.2: a filter starting with a wildcard does not match a topic starting with `$`.
    const system = topic.startsWith("$")

    this.walk(this.root, levels, 0, system, f)
  }

  private walk(at: Node<A>, levels: ReadonlyArray<string>, i: number, system: boolean, f: (subscriber: A) => void): void {
    const wildcards = !(system && i === 0)

    if (wildcards) at.rest.forEach(f)

    if (i === levels.length) {
      at.exact.forEach(f)

      return
    }

    const exact = at.children.get(levels[i] ?? "")

    if (exact !== undefined) this.walk(exact, levels, i + 1, system, f)

    const plus = wildcards ? at.children.get("+") : undefined

    if (plus !== undefined) this.walk(plus, levels, i + 1, system, f)
  }
}
