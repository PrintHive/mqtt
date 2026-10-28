import { describe, expect, it } from "vitest"
import { isValidFilter, TopicTree } from "../src/topic.ts"

const matched = (filters: ReadonlyArray<string>, topic: string): Array<string> => {
  const tree = new TopicTree<string>()

  for (const f of filters) tree.add(f, f)

  const out: Array<string> = []

  tree.match(topic, (f) => out.push(f))

  return out.sort()
}

/** A direct reading of section 4.7, written independently of the trie so the two can disagree. */
const oracle = (filter: string, topic: string): boolean => {
  if (topic.startsWith("$") && (filter.startsWith("+") || filter.startsWith("#"))) return false

  const f = filter.split("/")
  const t = topic.split("/")

  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") return true

    if (i >= t.length) return false

    if (f[i] !== "+" && f[i] !== t[i]) return false
  }

  return f.length === t.length
}

describe("TopicTree", () => {
  it("matches the examples in section 4.7.1", () => {
    const filters = ["sport/tennis/player1/#", "sport/#", "#", "sport/tennis/#", "sport/+", "+/+", "/+", "+", "sport/+/player1"]

    expect(matched(filters, "sport/tennis/player1")).toEqual(["#", "sport/#", "sport/+/player1", "sport/tennis/#", "sport/tennis/player1/#"])
    expect(matched(filters, "sport/tennis/player1/ranking")).toEqual(["#", "sport/#", "sport/tennis/#", "sport/tennis/player1/#"])
    expect(matched(filters, "sport")).toEqual(["#", "+", "sport/#"])
    expect(matched(filters, "sport/")).toEqual(["#", "+/+", "sport/#", "sport/+"])
    expect(matched(filters, "/finance")).toEqual(["#", "+/+", "/+"])
  })

  it("keeps wildcards at the first level away from topics starting with $", () => {
    expect(matched(["#", "+/monitor/Clients", "$SYS/#", "$SYS/monitor/+"], "$SYS/monitor/Clients")).toEqual(["$SYS/#", "$SYS/monitor/+"])
  })

  it("agrees with a direct reading of the spec on every filter and topic built from a small alphabet", () => {
    const levels = ["a", "b", "", "$x"]
    const topics = levels.flatMap((x) => [x, ...levels.flatMap((y) => [`${x}/${y}`, ...levels.map((z) => `${x}/${y}/${z}`)])]).filter((t) => t.length > 0)
    const filterLevels = [...levels, "+"]
    const bases = filterLevels.flatMap((x) => [x, ...filterLevels.flatMap((y) => [`${x}/${y}`, ...filterLevels.map((z) => `${x}/${y}/${z}`)])])
    const filters = [...new Set([...bases, ...bases.map((b) => `${b}/#`), "#"])].filter(isValidFilter)
    const tree = new TopicTree<string>()

    for (const f of filters) tree.add(f, f)

    for (const topic of topics) {
      const out: Array<string> = []

      tree.match(topic, (f) => out.push(f))
      expect(out.sort()).toEqual(filters.filter((f) => oracle(f, topic)).sort())
    }
  })

  it("stops delivering to a removed subscriber and prunes empty branches", () => {
    const tree = new TopicTree<string>()

    tree.add("a/+/c", "one")
    tree.add("a/#", "two")
    tree.remove("a/+/c", "one")
    tree.remove("a/#", "two")

    const out: Array<string> = []

    tree.match("a/b/c", (s) => out.push(s))
    expect(out).toEqual([])
    expect(tree).toEqual(new TopicTree<string>())
  })
})

describe("isValidFilter", () => {
  it.each([
    ["sport/tennis/#", true],
    ["#", true],
    ["+", true],
    ["sport/+/player1", true],
    ["sport/tennis#", false],
    ["sport/tennis/#/ranking", false],
    ["sport+", false],
    ["", false]
  ])("%s is %s", (filter, valid) => {
    expect(isValidFilter(filter)).toBe(valid)
  })
})
