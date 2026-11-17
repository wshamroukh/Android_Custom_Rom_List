#!/usr/bin/env python3
"""
rom_scout.py - keep Android_Custom_Rom_List/README.md in sync with the custom
Android ROM organisations that are actively developed on GitHub.

Zero third-party dependencies (Python 3.9+ stdlib only).

Pipeline
  1. DISCOVER  Repo search (forks included) for frameworks_base, Settings,
               frameworks_native, frameworks_opt_telephony, in date windows so
               the 1000-result search cap is never hit. Listed ROMs are always
               re-checked too.
  2. MANIFEST  Every org needs a manifest repo (android or *manifest*). Its
               newest branches are read: the AOSP / CLO tag (or LineageOS
               branch) gives the Android version; the revisions of the org's
               own projects give the ROM branches.
  3. ACTIVITY  Real commit dates of frameworks_base, Settings and other core
               repos ON those ROM branches (pushedAt alone is easy to fool).
  4. LATEST    Highest Android version that >= 5 active ROM manifests build
               (or --latest-android).
  5. CLASSIFY  Latest Android + active -> main list; latest-1 -> an
               "Android N-1" section at the bottom; older or inactive ->
               removed (with a safety cap). New orgs also pass the org /
               own-projects / variant checks.
  6. MERGE     Existing rows keep their exact text; rows move between the two
               sections; new rows go in alphabetically; empty link cells filled.

Outputs (in --out): README.proposed.md, README.diff, report.md, pr-body.md,
                    candidates.json, cache.json
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import difflib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.github.com"
UA = "rom-scout/1.0 (+https://github.com/wshamroukh/Android_Custom_Rom_List)"

# ---------------------------------------------------------------- patterns
FW_RE = re.compile(r"frameworks[_-]base(?![a-z])", re.I)
SETTINGS_RE = re.compile(r"packages[_-]apps[_-]settings(?![a-z])", re.I)
MANIFEST_RE = re.compile(r"^(android|manifests?|(android|platform|rom)[_-]manifest|.*[_-]manifest)$", re.I)
AOSP_RE = re.compile(
    r"^(android[_-]|platform[_-])?(frameworks|packages|vendor|device|build|system|hardware|"
    r"external|bootable|art|bionic|kernel|libcore|prebuilts|toolchain|bootable)[_-]", re.I)
# Core ROM repos used to measure real activity. Many ROMs host frameworks_base /
# Settings outside GitHub (Gitea, GitLab, own Gerrit) because of size limits, so
# activity is taken from ANY of these, not only frameworks_base.
CORE_RE = re.compile(
    r"^(android_|platform_)?(frameworks_(base|native|av|opt_telephony|libs_systemui)|"
    r"packages_apps_(settings|launcher3|systemui|updater|parts)|build_soong|build_make|build|"
    r"system_core|vendor_[a-z0-9]+)$", re.I)
REMOTE_RE = re.compile(r"<remote\b[^>]*?fetch\s*=\s*\"([^\"]+)\"", re.I)
SKIP_HOSTS = ("github.com", "googlesource.com", "codelinaro.org", "gitlab.com/LineageOS")
PUBLIC_HOSTS = {"gitlab.com", "gitea.com", "codeberg.org", "bitbucket.org", "git.codelinaro.org",
                "sourceforge.net", "gitee.com"}
CANON_MANIFESTS = {"android", "manifest", "manifests", "android_manifest", "platform_manifest", "rom_manifest"}
MANIFEST_NAMES = ["android", "manifest", "android_manifest", "platform_manifest", "manifests"]
README_FILES = ["README.md", "README.mkdn", "README.markdown", "README"]

TG_RE = re.compile(r"https?://(?:www\.)?(?:t\.me|telegram\.me|telegram\.dog)/(\+[\w-]+|joinchat/[\w-]+|[A-Za-z][A-Za-z0-9_]{3,})", re.I)
DC_RE = re.compile(r"https?://(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite)/([\w-]+)", re.I)
TG_IGNORE = {"share", "s", "iv", "addstickers", "setlanguage", "proxy", "socks"}


def log(*a):
    print(f"[{dt.datetime.now():%H:%M:%S}]", *a, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- GitHub client
class QueryTooHeavy(Exception):
    pass


class GitHub:
    def __init__(self, token: str):
        self.token = token
        self._last_search = 0.0

    def _req(self, method, url, body=None, tries=6):
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(tries):
            req = urllib.request.Request(url, data=data, method=method, headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": UA,
                **({"Content-Type": "application/json"} if data else {}),
            })
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                txt = e.read().decode(errors="replace")[:300]
                if e.code in (502, 504) and url.endswith("/graphql"):
                    if attempt >= 1:
                        raise QueryTooHeavy(f"HTTP {e.code}")
                    log(f"HTTP {e.code} (GraphQL timeout) - retrying once in 5s")
                    time.sleep(5)
                    continue
                if e.code in (403, 429) or e.code >= 500:
                    ra = e.headers.get("Retry-After")
                    reset = e.headers.get("X-RateLimit-Reset")
                    remaining = e.headers.get("X-RateLimit-Remaining")
                    if ra:
                        wait = int(ra) + 1
                    elif remaining == "0" and reset:
                        wait = max(5, int(reset) - int(time.time()) + 2)
                    else:
                        wait = min(300, 15 * (2 ** attempt))
                    log(f"HTTP {e.code} ({txt[:80]!r}) - sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if e.code == 422:   # search query rejected
                    log(f"HTTP 422 for {url}: {txt}")
                    return None
                raise RuntimeError(f"HTTP {e.code} {url}: {txt}")
            except (urllib.error.URLError, TimeoutError) as e:
                wait = 10 * (attempt + 1)
                log(f"network error {e} - retry in {wait}s")
                time.sleep(wait)
        raise RuntimeError(f"giving up on {url}")

    def search_repos(self, q, page):
        # authenticated search limit: 30 req/min  -> keep >= 2.2 s apart
        gap = time.time() - self._last_search
        if gap < 2.2:
            time.sleep(2.2 - gap)
        self._last_search = time.time()
        url = f"{API}/search/repositories?" + urllib.parse.urlencode(
            {"q": q, "per_page": 100, "page": page, "sort": "updated", "order": "desc"})
        return self._req("GET", url)

    def graphql(self, query, variables=None):
        r = self._req("POST", f"{API}/graphql", {"query": query, "variables": variables or {}})
        errs = [e for e in (r.get("errors") or []) if e.get("type") != "NOT_FOUND"]
        if errs:
            log("GraphQL errors:", json.dumps(errs)[:400])
        return r.get("data") or {}


def get_token():
    tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if tok:
        return tok.strip()
    try:
        return subprocess.check_output(["gh", "auth", "token"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        sys.exit("No token: export GITHUB_TOKEN=<classic PAT, no scopes needed> or run `gh auth login`.")


# ---------------------------------------------------------------- 1. discover
def iso(d: dt.datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def discover(gh: GitHub, terms, start: dt.datetime, end: dt.datetime, window_days=7):
    """Return {owner_login: {'type':..., 'hits': {repo_name: pushed_at}}}."""
    owners: dict[str, dict] = {}

    def run_window(term, a, b, depth=0):
        q = f"{term} in:name fork:true pushed:{iso(a)}..{iso(b)}"
        first = gh.search_repos(q, 1)
        if first is None:
            return
        total = first.get("total_count", 0)
        if total > 1000 and (b - a) > dt.timedelta(minutes=30):
            mid = a + (b - a) / 2
            log(f"  {term}: {total} hits in {iso(a)}..{iso(b)} -> splitting")
            run_window(term, a, mid, depth + 1)
            run_window(term, mid, b, depth + 1)
            return
        if first.get("incomplete_results"):
            log(f"  warning: GitHub returned incomplete results for {q}")
        items = list(first.get("items", []))
        pages = min(10, (total + 99) // 100)
        for p in range(2, pages + 1):
            r = gh.search_repos(q, p)
            items += (r or {}).get("items", [])
        log(f"  {term}: {iso(a)[:10]}..{iso(b)[:10]} -> {total} hits")
        for it in items:
            name = it["name"]
            if not re.search(re.escape(term.lower()) + r"(?![a-z])", name.lower().replace("-", "_")):
                continue
            o = it["owner"]
            ent = owners.setdefault(o["login"], {"type": o.get("type", "?"), "hits": {}})
            ent["hits"][name] = it.get("pushed_at")

    for term in terms:
        a = start
        while a < end:
            b = min(end, a + dt.timedelta(days=window_days))
            run_window(term, a, b)
            a = b
    return owners


# ---------------------------------------------------------------- 3. verify
Q_OWNER = """
query($login:String!, $after:String){
  rateLimit{ remaining resetAt }
  repositoryOwner(login:$login){
    __typename login url
    ... on Organization { name description websiteUrl createdAt }
    ... on User { name bio websiteUrl }
    repositories(first:100, after:$after, ownerAffiliations:[OWNER], orderBy:{field:PUSHED_AT,direction:DESC}){
      totalCount pageInfo{ hasNextPage endCursor }
      nodes{ name pushedAt isArchived isFork }
    }
  }
}"""


def gql_str(s):
    return json.dumps(s)


def readme_fields():
    return " ".join(f'r{i}: object(expression:{gql_str("HEAD:" + f)}){{ ... on Blob {{ text }} }}'
                    for i, f in enumerate(README_FILES))


def gql_parts(gh, parts, login, batch=3):
    """Run aliased query parts in small batches; on GraphQL timeout split down to single parts."""
    out = {}
    def run(chunk):
        try:
            out.update(gh.graphql("query{ " + " ".join(chunk) + " }"))
        except QueryTooHeavy:
            if len(chunk) > 1:
                for p in chunk:
                    run([p])
            else:
                lite = re.sub(r"(first|last):100\)", r"\1:20)", chunk[0])      # fewer branches
                lite = lite.replace("... on Tree { entries { name object { ... on Blob { text } } } } ", "")  # root XMLs only
                if lite != chunk[0]:
                    try:
                        out.update(gh.graphql("query{ " + lite + " }"))
                        return
                    except QueryTooHeavy:
                        pass
                log(f"  {login}: skipped one sub-query after repeated GitHub timeouts")
    for i in range(0, len(parts), batch):
        run(parts[i:i + batch])
    return out


REF_FIELDS = "name target{ ... on Commit { committedDate } }"
TREE = ('object(expression:{expr}){{ ... on Tree {{ entries {{ name object {{ ... on Blob {{ text }} '
        '... on Tree {{ entries {{ name object {{ ... on Blob {{ text }} }} }} }} }} }} }} }}')


def _dt(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def _refs(node):
    """(branch, datetime) pairs from a repo node holding a/z listings, defaultBranchRef and b<N> refs."""
    out = []
    for key, val in (node or {}).items():
        if key in ("a", "z") and val:
            out += [(r["name"], (r.get("target") or {}).get("committedDate")) for r in val.get("nodes") or []]
        elif (key == "defaultBranchRef" or re.fullmatch(r"b\d+", key)) and val:
            out.append((val["name"], (val.get("target") or {}).get("committedDate")))
    return [(n, _dt(c)) for n, c in out if c]


def _xmls(tree_obj):
    xs, stack = [], list(((tree_obj or {}).get("entries")) or [])
    while stack:
        e = stack.pop()
        o = e.get("object") or {}
        if o.get("entries"):
            stack += o["entries"]
        elif e["name"].endswith(".xml") and o.get("text"):
            xs.append(o["text"])
    return xs


def verify_owner(gh: GitHub, login: str, hint_repos: list[str], now: dt.datetime, deep: bool = False):
    """Collect everything classification needs. deep=True for listed / allowlisted owners."""
    d = gh.graphql(Q_OWNER, {"login": login}).get("repositoryOwner")
    if not d:
        return {"login": login, "error": "owner not found"}
    repos = list(d["repositories"]["nodes"])
    page = d["repositories"]["pageInfo"]
    while page["hasNextPage"] and len(repos) < 2000:
        dn = gh.graphql(Q_OWNER, {"login": login, "after": page["endCursor"]}).get("repositoryOwner")
        if not dn:
            break
        repos += dn["repositories"]["nodes"]
        page = dn["repositories"]["pageInfo"]
    names = [r["name"] for r in repos]
    cutoff_60 = now - dt.timedelta(days=60)
    # A ROM must have a manifest repo named "android" or containing "manifest";
    # local_manifest(s) repos belong to device / personal orgs and do not count.
    mani = [n for n in names if (n.lower() == "android" or "manifest" in n.lower())
            and not re.search(r"local_manifest|kernel|device", n.lower().replace("-", "_"))]
    info = {
        "login": d["login"], "type": d["__typename"], "url": d["url"],
        "name": (d.get("name") or "").strip() or d["login"],
        "website": d.get("websiteUrl") or "",
        "repoCount": d["repositories"]["totalCount"],
        "aospLikeRepos": sum(1 for n in names if AOSP_RE.match(n)),
        "vendorBrands": vendor_brands(names),
        "activeRepos60d": sum(1 for r in repos if r["pushedAt"] and _dt(r["pushedAt"]) >= cutoff_60),
        "manifests": mani, "variants": [], "links": [],
    }
    if not deep and d["__typename"] != "Organization":
        info["skip"] = "personal account"
        return info
    if not deep and not mani:
        info["skip"] = "no manifest / android repo (private or personal org)"
        return info

    # ---- B: manifest repos - branches with real commit dates, READMEs for links
    parts = [f'm{i}: repository(owner:{gql_str(login)}, name:{gql_str(n)}){{ name defaultBranchRef{{ {REF_FIELDS} }} '
             f'a: refs(refPrefix:"refs/heads/", first:100){{ nodes{{ {REF_FIELDS} }} }} '
             f'z: refs(refPrefix:"refs/heads/", last:100){{ nodes{{ {REF_FIELDS} }} }} {readme_fields()} }}'
             for i, n in enumerate(mani[:4])]
    parts.append(f'p: repository(owner:{gql_str(login)}, name:".github"){{ '
                 f'r0: object(expression:"HEAD:profile/README.md"){{ ... on Blob {{ text }} }} }}')
    d2 = gql_parts(gh, parts, login)
    texts, cands, defaults = [d.get("description") or "", d.get("bio") or ""], [], {}
    for k, v in d2.items():
        if not v:
            continue
        if k == "p" and v.get("r0"):
            texts.append(v["r0"].get("text") or "")
        if k.startswith("m"):
            texts += [v[f"r{i}"]["text"] for i in range(len(README_FILES)) if v.get(f"r{i}") and v[f"r{i}"].get("text")]
            if v.get("defaultBranchRef"):
                defaults[v["name"]] = v["defaultBranchRef"]["name"]
            cands += [(t, v["name"], br) for br, t in _refs(v) if t <= now + dt.timedelta(days=1)]
    info["links"] = extract_links(texts, d.get("websiteUrl") or "")

    # ---- C: read the manifest XML on the newest two branches + the default branch
    cands.sort(reverse=True)
    pick = []
    for _, m, br in cands:
        if (m, br) not in pick:
            pick.append((m, br))
        if len(pick) == 2:
            break
    if pick and defaults.get(pick[0][0]) and (pick[0][0], defaults[pick[0][0]]) not in pick:
        pick.append((pick[0][0], defaults[pick[0][0]]))
    named = sorted(((android_version(br) or 0, t, m, br) for t, m, br in cands), reverse=True)
    for ver, _, m, br in named:                  # e.g. "seventeen" / "lineage-24.0" even if not the newest push
        if len(pick) >= 5 or ver < 15:
            break
        if (m, br) not in pick:
            pick.append((m, br))
    when = {(m, br): t for t, m, br in cands}
    parts = [f't{j}: repository(owner:{gql_str(login)}, name:{gql_str(m)}){{ tree: {TREE.format(expr=gql_str(br + ":"))} }}'
             for j, (m, br) in enumerate(pick)]
    d3 = gql_parts(gh, parts, login, batch=1) if parts else {}
    for j, (m, br) in enumerate(pick):
        node = d3.get(f"t{j}") or {}
        an = analyse_manifest(_xmls(node.get("tree")), login, br)
        an.update({"manifest": m, "branch": br, "manifestCommit": iso(when[(m, br)])})
        info["variants"].append(an)

    # ---- D: real commits of core ROM repos on the branches the manifest points at
    pool = set(names + hint_repos)
    fw = sorted(n for n in pool if FW_RE.search(n.replace("-", "_"))) or \
        ["android_frameworks_base", "platform_frameworks_base", "frameworks_base"]
    st = sorted(n for n in pool if SETTINGS_RE.search(n.replace("-", "_"))) or \
        ["android_packages_apps_Settings", "platform_packages_apps_Settings", "packages_apps_Settings"]
    core = [r["name"] for r in repos if CORE_RE.match(r["name"].replace("-", "_")) and not r["isArchived"]]
    check = list(dict.fromkeys(fw[:3] + st[:3] + core[:6]))
    info["coreRepos"] = len(core)
    rom_branches = list(dict.fromkeys(b for an in info["variants"] for b in an["romBranches"]))[:6]
    parts = []
    for i, n in enumerate(check):
        refs = " ".join(f'b{j}: ref(qualifiedName:{gql_str("refs/heads/" + b)}){{ {REF_FIELDS} }}'
                        for j, b in enumerate(rom_branches))
        if not rom_branches:
            refs = (f'a: refs(refPrefix:"refs/heads/", first:100){{ nodes{{ {REF_FIELDS} }} }} '
                    f'z: refs(refPrefix:"refs/heads/", last:100){{ nodes{{ {REF_FIELDS} }} }}')
        parts.append(f'k{i}: repository(owner:{gql_str(login)}, name:{gql_str(n)}){{ name isArchived '
                     f'defaultBranchRef{{ {REF_FIELDS} }} {refs} }}')
    d4 = gql_parts(gh, parts, login)
    per_branch, any_best = {}, None
    for k, v in d4.items():
        if not k.startswith("k") or not v or v.get("isArchived"):
            continue
        for br, t in _refs(v):
            if t > now + dt.timedelta(days=1):          # bogus future clock
                continue
            if any_best is None or t > any_best[0]:
                any_best = (t, br, v["name"])
            if br in rom_branches and (br not in per_branch or t > per_branch[br][0]):
                per_branch[br] = (t, v["name"])
    for an in info["variants"]:
        hits = [(per_branch[b][0], b, per_branch[b][1]) for b in an["romBranches"] if b in per_branch]
        best = max(hits) if hits else None
        an.update({"lastCommit": iso(best[0]) if best else None,
                   "lastCommitBranch": best[1] if best else None, "lastCommitRepo": best[2] if best else None})
    info["anyCommit"] = ({"lastCommit": iso(any_best[0]), "lastCommitBranch": any_best[1],
                          "lastCommitRepo": any_best[2]} if any_best else None)
    info["frameworksBase"] = [n for n in fw if n in pool]
    info["settings"] = [n for n in st if n in pool]
    return info


# ---------------------------------------------------------------- manifest analysis
ATTR_RE = re.compile(r'([\w-]+)\s*=\s*"([^"]*)"')


def _host(url):
    u = url.strip()
    if "://" not in u and "@" in u and ":" in u:          # git@host:path
        u = "ssh://" + u.replace(":", "/", 1)
    h = urllib.parse.urlparse(u).netloc
    return h.split("@")[-1].split(":")[0].lower()


def _domain_matches(host, login):
    """git.infinityx.org <-> ProjectInfinity-X : the custom domain belongs to this ROM."""
    lg = re.sub(r"[^a-z0-9]", "", login.lower())
    for label in re.split(r"[.\-]", host.lower()):
        if len(label) >= 4 and label not in {"gitea", "gerrit", "review", "code", "source", "android", "aosp", "mirror"} \
                and (label in lg or lg in label):
            return True
    return False


AOSP_TAG_RE = re.compile(r"android-(\d{2})\.\d+\.\d+_r\d+|android(\d{2})-(?:qpr\d+-)?(?:s\d+-)?(?:release|beta)", re.I)
CLO_RE = re.compile(r"\bLA\.QSSI\.(\d{2})\.\d", re.I)
LINEAGE_REV_RE = re.compile(r"\blineage-(\d{2})\.\d")


def detect_version(xml_texts, branch, rom_branches):
    """Android version a manifest builds: AOSP tag > CLO tag > LineageOS branch > branch names."""
    blob = "\n".join(xml_texts)
    tags = [int(a or b) for a, b in AOSP_TAG_RE.findall(blob)]
    if tags:
        return max(tags), "AOSP tag"
    clo = [int(x) for x in CLO_RE.findall(blob)]
    if clo:
        return max(clo), "CLO tag"
    lin = [int(x) - 7 for x in LINEAGE_REV_RE.findall(blob)]
    if lin:
        return max(lin), "LineageOS branch"
    for b in [branch] + list(rom_branches):
        v = android_version(b)
        if v:
            return v, f"branch name {b}"
    return None, None


def analyse_manifest(xml_texts, login, branch):
    """Own projects, own git servers, the branches the ROM's own repos are on, and the Android version."""
    remotes, default, projects = {}, {}, []
    for t in xml_texts:
        for tag, attrs in re.findall(r"<(remote|default|project)\b([^>]*)>", t, re.I):
            a = dict(ATTR_RE.findall(attrs))
            tag = tag.lower()
            if tag == "remote" and a.get("name"):
                remotes[a["name"]] = (a.get("fetch", ""), a.get("revision"))
            elif tag == "default":
                default.update({k: v for k, v in a.items() if k in ("remote", "revision")})
            elif tag == "project" and a.get("name"):
                projects.append((a["name"], a.get("remote"), a.get("revision")))
    own, external, lg, revs = 0, set(), login.lower(), {}
    for name, rem, rev in projects:
        rname = rem or default.get("remote") or ""
        fetch, rrev = remotes.get(rname, ("", None))
        full = (fetch.rstrip("/") + "/" + name).lower()
        host = _host(fetch) if "://" in fetch or "@" in fetch else "github.com"
        mine = False
        if f"github.com/{lg}/" in full or (host == "github.com" and name.lower().startswith(lg + "/")):
            mine = True
        elif host and host != "github.com" and not any(x in full for x in SKIP_HOSTS) and \
                (f"/{lg}/" in full or (host not in PUBLIC_HOSTS and _domain_matches(host, lg))):
            mine = True                       # own git server (custom domain) or own space on a public host
            external.add(host)
        if mine:
            own += 1
            r = (rev or rrev or default.get("revision") or "").replace("refs/heads/", "")
            if r and not r.startswith("refs/") and not re.fullmatch(r"[0-9a-f]{40}", r):
                revs[r] = revs.get(r, 0) + 1
    rom_branches = [b for b, _ in sorted(revs.items(), key=lambda x: -x[1])][:3]
    if branch not in rom_branches:
        rom_branches.append(branch)          # repos usually share the manifest branch name
    ver, src = detect_version(xml_texts, branch, rom_branches)
    return {"own": own, "total": len(projects), "external": sorted(external),
            "romBranches": rom_branches, "version": ver, "versionSource": src}


# ---------------------------------------------------------------- 5. links
def extract_links(texts, website):
    tg, dc, seen = [], [], set()
    for t in texts:
        for m in TG_RE.finditer(t or ""):
            h = m.group(1)
            if h.lower() in TG_IGNORE or h.lower().endswith("bot"):
                continue
            u = f"https://t.me/{h}"
            if u.lower() not in seen:
                seen.add(u.lower()); tg.append(u)
        for m in DC_RE.finditer(t or ""):
            u = f"https://discord.gg/{m.group(1)}"
            if u.lower() not in seen:
                seen.add(u.lower()); dc.append(u)
    out = tg[:4] + dc[:2]
    w = website.strip()
    if w and "github.com" not in w.lower():
        if not w.startswith("http"):
            w = "https://" + w
        out.append(w.rstrip("/"))
    return out


# ---------------------------------------------------------------- 4. classify
CACHE_VER = 4
GENERIC_VENDORS = re.compile(
    r"^(qcom.*|qti|google.*|gms|pixel.*|gapps|opengapps|microg|extras?|interfaces|patches|patchset|crowdin|partner.*|"
    r"addons?|prebuilts?|firmware|lindroid|custom|support|codeaurora|clo|aosp|ota|updater|overlays?|themes?|apps|"
    r"bootanimation|priv|private|signing|keys|certs?|oplus|xiaomi|oneplus|samsung|motorola|nothing|sony|lenovo|"
    r"realme|asus|fairphone|mediatek|mtk|exynos|sprd|unisoc|rockchip|amlogic|nvidia|x86|intel|broadcom|sm\d+|"
    r"lineage|gsi|bluetooth|camera|dolby|miui.*|hyperos|viper.*|certification|bcr|fontbox|spoofing|lawnchair|"
    r"settings|.*settings|consumerirapp|.*ota|gcam.*|aperture|kprofiles|parts|launcher.*|fonts?|sounds?|"
    r"wallpapers?|icons?|branding|build|tools|utils|common|config|sepolicy|signed|release|motionphoto)$")
GENERIC_TOKENS = {"project", "prjkt", "the", "os", "aosp", "android", "rom", "roms", "oss", "ui", "staging",
                  "stagging", "team", "org", "official", "lab", "labs", "dev", "devs", "x"}
CTX = {"families": {}, "brands": {}, "latest": 17}


def vendor_brands(names):
    out = set()
    for n in names:
        m = re.match(r"^(?:android_|platform_)?vendor_([a-z0-9]+)$", n.lower().replace("-", "_"))
        if m and not GENERIC_VENDORS.match(m.group(1)):
            out.add(m.group(1))
    return sorted(out)


def family(s):
    """'Project-Mist-OS' -> 'mist', 'crDroid11' -> 'crdroid', 'SuperiorOS-Violet' -> 'superior'."""
    for t in re.split(r"[-_ ./()\[\]]+", s or ""):
        t = t.lower()
        if not t or t in GENERIC_TOKENS:
            continue
        t = re.sub(r"^(project|prjkt|the)", "", t)
        for _ in range(2):
            t = re.sub(r"(android|aosp|oss|os|ui|rom|official|revived|staging|stagging|project|\d+)$", "", t)
        return t if len(t) >= 4 else None
    return None


WORDS = {"ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16,
         "seventeen": 17, "eighteen": 18}


def android_version(branch):
    """Best-effort Android version from a branch name; None when unsure (never guess from kernel/ROM versions)."""
    b = (branch or "").lower()
    m = re.search(r"(?:lineage|cm)[-_]?(\d{2})", b)
    if m:
        return int(m.group(1)) - 7
    for w, n in WORDS.items():
        if re.search(rf"(^|[^a-z]){w}([^a-z]|$)", b):
            return n
    if b in ("q", "r", "s", "t", "u", "v"):
        return {"q": 10, "r": 11, "s": 12, "t": 13, "u": 14, "v": 15}[b]
    m = re.search(r"(?:android|aosp)[-_]?(\d{1,2})(?:[._]\d+)*", b)       # android-2.3.7, aosp-5.1.1, android17
    if m:
        return int(m.group(1))
    m = re.match(r"^[a-z]?(\d{2})(?:[._-]|$)", b)                          # 17, 16-qpr2, 13.0-x, t13.0
    if m and 10 <= int(m.group(1)) <= 17:
        return int(m.group(1))
    for tok in re.split(r"[/_-]", b):                                     # rr-17.0, nexus7-18.1, petal-23.2
        mm = re.fullmatch(r"(\d{2})(?:\.\d+)*", tok)
        if mm and 10 <= int(mm.group(1)) <= 17:
            return int(mm.group(1))
        if mm and 18 <= int(mm.group(1)) <= 25:
            return int(mm.group(1)) - 7
    return None


DEVICE_RE = re.compile(
    r"(^|[-_])(sm\d{4}|sdm\d{3}|msm\d{4}|mt\d{4}|sd\d{3}|\d{4}[-_]?lab|op\d{1,2}[a-z]?|oneplus|oplus|xiaomi|redmi|poco|"
    r"lenovo|samsung|galaxy|motorola|moto|realme|nothing|pixel\d|nord\d*|giulia|pipa|marble|peridot|nuwa|"
    r"spes|stone|sweet|surya|alioth|munch|garnet|lavender|ginkgo|vayu|raphael|sapphire|r8q|gts\d+[a-z]*)([-_]|$)", re.I)


def _family_hit(f):
    if f in CTX["families"]:
        return CTX["families"][f]
    for k, name in CTX["families"].items():          # caesiumdesktop -> caesium
        if len(k) >= 5 and f.startswith(k):
            return name
    return None


def related_flags(v):
    """Signals that a new org is a build/port/staging copy of a ROM already on the list, or on an old base."""
    flags = []
    for f in {family(v["login"]), family(v.get("name"))} - {None}:
        hit = _family_hit(f)
        if hit:
            flags.append(f"same family as listed {hit} (staging / device port / fork?)")
            break
    if DEVICE_RE.search(v["login"]):
        flags.append("device / SoC name in org (device port?)")
    for m in v.get("manifests", []):
        pre = re.split(r"[-_]?manifests?", m.lower())[0]
        hit = _family_hit(family(pre) or "") if pre and m.lower() not in CANON_MANIFESTS else None
        if hit:
            flags.append(f"manifest {m} is a build of listed {hit}")
            break
    own = [b for b in v.get("vendorBrands", []) if CTX["brands"].get(b, v["login"].lower()) == v["login"].lower()]
    other = sorted({CTX["brands"][b] for b in v.get("vendorBrands", []) if b in CTX["brands"]
                    and CTX["brands"][b] != v["login"].lower()})
    if other and not own:
        flags.append(f"no own vendor brand; uses vendor_{'/'.join(b for b in v['vendorBrands'] if b in CTX['brands'])} of {', '.join(other)}")
    return flags


def review_sections(items, tbl, cols):
    groups = [("new", "New - needs human review: possible new ROMs"),
              ("variant", "New - needs human review: variants / ports / staging of listed ROMs")]
    out = []
    for k, title in groups:
        g = [v for v in items if v.get("reviewKind", "new") == k]
        out += [f"\n## {title} ({len(g)})\n", tbl(g, cols)]
    return out


def choose(v, days, now):
    """Pick the manifest branch that represents the ROM: highest Android version that is still active."""
    cut = now - dt.timedelta(days=days)
    vs = [x for x in v.get("variants", []) if x.get("version")]
    act = [x for x in vs if x.get("lastCommit") and _dt(x["lastCommit"]) >= cut]
    pick = max(act or vs, key=lambda x: (x["version"], x.get("lastCommit") or ""), default=None)
    if pick is None and v.get("variants"):
        pick = max(v["variants"], key=lambda x: x.get("lastCommit") or x.get("manifestCommit") or "")
    p = pick or {}
    v["androidVersion"], v["versionSource"] = p.get("version"), p.get("versionSource")
    v["manifestBranch"] = f"{p['manifest']}@{p['branch']}" if p else None
    v["manifestOwnProjects"], v["externalGit"] = p.get("own", 0), p.get("external", [])
    v["romBranches"] = p.get("romBranches", [])
    src = p if p.get("lastCommit") else (v.get("anyCommit") or {})
    v["activitySource"] = "ROM branch" if p.get("lastCommit") else ("any branch" if src else None)
    for k in ("lastCommit", "lastCommitBranch", "lastCommitRepo"):
        v[k] = src.get(k)


def tier(ver):
    L = CTX["latest"]
    if ver is None:
        return "unknown"
    return "latest" if ver >= L else ("previous" if ver == L - 1 else "old")


def classify(v, days, now, existing, deny, allow):
    login = v["login"].lower()
    L = CTX["latest"]
    if v.get("error"):
        return "ERROR", v["error"]
    age = (now - _dt(v["lastCommit"])).days if v.get("lastCommit") else None
    ver, t = v.get("androidVersion"), tier(v.get("androidVersion"))
    seen = f"last ROM commit {age}d ago" if age is not None else "no ROM commit found"
    vtxt = f"Android {ver}" if ver else "Android version unknown"

    if login in existing:
        if login in allow:
            return "KEEP", f"allowlisted; {vtxt}; {seen}"
        active = (age is not None and age <= days) or v.get("activeRepos60d", 0) >= 3
        if active and not (age is not None and age <= days):
            seen += f" (kept: {v.get('activeRepos60d', 0)} repos pushed in 60d)"
        if not active:
            return "REMOVE", f"inactive: {seen}, {v.get('activeRepos60d', 0)} repos pushed in 60d"
        if t == "latest":
            return "MAIN", f"{vtxt}; {seen}"
        if t == "previous":
            return "PREV", f"{vtxt}; {seen}"
        if t == "old":
            return "REMOVE", f"{vtxt} (older than Android {L - 1}); {seen}"
        return "KEEP", f"{vtxt} (no readable manifest) - left where it is; {seen}"

    if v.get("skip"):
        return "REJECT", v["skip"]
    if login in deny:
        return "REJECT", "denylisted"
    if age is None or age > days:
        return "REJECT", seen
    if login in allow:
        if t == "old":
            return "REJECT", f"allowlisted, but {vtxt} is older than Android {L - 1}"
        v["target"] = "prev" if t == "previous" else "main"
        return "ACCEPT", f"allowlisted; {vtxt}"
    own = v.get("manifestOwnProjects", 0)
    if v["type"] != "Organization":
        return "REJECT", "personal account"
    if v.get("repoCount", 0) >= 100 and own == 0 and v.get("activeRepos60d", 0) >= 0.9 * v["repoCount"]:
        return "REJECT", "mirror / CI org (every repo pushed recently, no ROM manifest)"
    if not v["manifests"]:
        return "REJECT", "no manifest / android repo (private or personal org)"
    if own < 5:
        return "REJECT", f"manifest pulls only {own} own projects (device / build org)"
    if t == "old":
        return "REJECT", f"{vtxt} (older than Android {L - 1})"
    mf = v["manifests"]
    if len(mf) >= 2 and not any(m.lower().replace("-", "_") in CANON_MANIFESTS for m in mf):
        v["reviewKind"] = "variant"
        return "REVIEW", f"{vtxt}; several other-ROM manifests ({', '.join(mf[:3])}) - looks like a builder org"
    flags = related_flags(v)
    if flags:
        v["reviewKind"] = "variant"
        return "REVIEW", f"{vtxt}; " + "; ".join(flags)
    if t == "unknown":
        return "REVIEW", f"{vtxt} - could not read it from the manifest"
    if own >= 15 and v["aospLikeRepos"] >= 8:
        v["target"] = "main" if t == "latest" else "prev"
        return "ACCEPT", f"{vtxt}; {seen}; manifest {v.get('manifestBranch')} pulls {own} own projects"
    reasons = [f"manifest pulls only {own} own projects"] if own < 15 else []
    if v["aospLikeRepos"] < 8:
        reasons.append(f"only {v['aospLikeRepos']} AOSP-layout repos")
    return "REVIEW", f"{vtxt}; " + "; ".join(reasons)


def detect_latest(results, days, now, min_roms):
    """Highest Android version that at least `min_roms` active ROM manifests already build."""
    cut = now - dt.timedelta(days=days)
    counts = {}
    for v in results.values():
        if v.get("error") or v.get("skip") or not v.get("androidVersion") or v.get("activitySource") != "ROM branch":
            continue
        if v.get("lastCommit") and _dt(v["lastCommit"]) >= cut:
            counts[v["androidVersion"]] = counts.get(v["androidVersion"], 0) + 1
    ok = [k for k, n in counts.items() if n >= min_roms and k <= 30]
    return (max(ok) if ok else None), dict(sorted(counts.items(), reverse=True))


# ---------------------------------------------------------------- README parse / merge
GH_OWNER_RE = re.compile(r"https?://github\.com/([A-Za-z0-9-_.]+)", re.I)
TD_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"https?://[^\s,<>\"')\]]+")


def _cells(line):
    """Return (style, cells) for one table row line, or None."""
    if re.search(r"<td", line, re.I):
        raw = TD_RE.findall(line)
        if len(raw) < 2:
            return None
        cells = []
        for k, c in enumerate(raw):
            hrefs = re.findall(r'href="([^"]+)"', c)
            txt = re.sub(r"\s+", " ", TAG_RE.sub(" ", c)).strip()
            cells.append(", ".join(dict.fromkeys(hrefs)) if hrefs and k > 0 else txt)
        return "html", cells
    if "|" in line:
        body = line.strip()
        outer = body.startswith("|")
        body = body.strip("|")
        cells = [c.strip() for c in body.split("|")]
        if len(cells) < 2 or set("".join(cells)) <= set("-: "):
            return None
        return ("md-outer" if outer else "md-bare"), cells
    return None


HEAD_RE = re.compile(r"^\s*(#{1,6}\s|<h[1-6][\s>])", re.I)
ANDROID_HEAD_RE = re.compile(r"^(\s*(?:#{1,6}\s*|<h[1-6][^>]*>\s*)Android\s*)(\d{1,2})\b", re.I)
TABLEISH_RE = re.compile(r"\||</?(table|thead|tbody|tr|th)\b", re.I)
CLOSING_RE = re.compile(r"^\s*</(table|tbody)>", re.I)


def parse_readme(text):
    rows, sec, sec_ver = [], -1, None
    for i, line in enumerate(text.splitlines()):
        if HEAD_RE.match(line):
            sec = i
            m = ANDROID_HEAD_RE.match(line)
            sec_ver = int(m.group(2)) if m else None
            continue
        r = _cells(line)
        if not r:
            continue
        style, cells = r
        idx = next((k for k, c in enumerate(cells) if GH_OWNER_RE.search(c)), None)
        if idx is None:
            continue          # header row
        o = GH_OWNER_RE.search(cells[idx])
        name = TAG_RE.sub("", cells[0]).strip() if idx > 0 else ""
        links = cells[idx + 1] if len(cells) > idx + 1 else ""
        rows.append({"line": i, "style": style, "raw": line, "name": name, "url": o.group(0),
                     "owner": o.group(1).lower(), "links": TAG_RE.sub("", links).strip(),
                     "sec": sec, "secVer": sec_ver})
    main_sec = rows[0]["sec"] if rows else None
    for r in rows:
        r["section"] = "main" if r["sec"] == main_sec else ("prev" if r["secVer"] else "other")
    return rows


def _block(lines, first, last):
    """Bounds of the table around rows first..last: (start incl. header lines, end incl. closing tags)."""
    s = first
    while s - 1 >= 0 and lines[s - 1].strip() and not HEAD_RE.match(lines[s - 1]) and TABLEISH_RE.search(lines[s - 1]):
        s -= 1
    e = last
    while e + 1 < len(lines) and CLOSING_RE.match(lines[e + 1]):
        e += 1
    return s, e


def _section_span(lines, h):
    """Heading line h + blank lines + its table + trailing blank lines."""
    i = h + 1
    while i < len(lines) and not lines[i].strip():
        i += 1
    while i < len(lines) and lines[i].strip() and not HEAD_RE.match(lines[i]) and \
            (TABLEISH_RE.search(lines[i]) or CLOSING_RE.match(lines[i]) or _cells(lines[i])):
        i += 1
    while i < len(lines) and not lines[i].strip():
        i += 1
    return range(h, i)


def merge(text, rows, place, new_entries, fills, latest):
    """place: owner -> main | prev | remove | keep. New entries carry e['target'] = main | prev."""
    lines = text.splitlines()
    for r in rows:                                   # fill empty link cells only
        if r["owner"] in fills and not r["links"] and fills[r["owner"]]:
            lines[r["line"]] = render_row(r, r["name"], r["url"], fills[r["owner"]])
        r["raw"] = lines[r["line"]]
    main = [r for r in rows if r["section"] == "main"]
    prev = [r for r in rows if r["section"] == "prev"]
    tpl = main[0]
    first, last = main[0]["line"], main[-1]["line"]
    start, end = _block(lines, first, last)
    header, footer = lines[start:first], lines[last + 1:end + 1]

    def new_line(e):
        return render_row(tpl, e["name"], f"https://github.com/{e['login']}", e.get("links", []))

    incoming_main = [(r["name"], r["raw"]) for r in prev if place.get(r["owner"]) == "main"] + \
                    [(e["name"], new_line(e)) for e in new_entries if e.get("target") == "main"]
    incoming_main.sort(key=lambda x: x[0].casefold())
    prev_rows = [(r["name"], r["raw"]) for r in prev if place.get(r["owner"], "keep") in ("prev", "keep")] + \
                [(r["name"], r["raw"]) for r in main if place.get(r["owner"]) == "prev"] + \
                [(e["name"], new_line(e)) for e in new_entries if e.get("target") == "prev"]
    prev_rows.sort(key=lambda x: x[0].casefold())

    by_line = {r["line"]: r for r in main}
    body = []
    for i in range(first, last + 1):
        r = by_line.get(i)
        if r is None:
            body.append(lines[i])
            continue
        if place.get(r["owner"], "keep") not in ("main", "keep"):
            continue
        while incoming_main and incoming_main[0][0].casefold() < r["name"].casefold():
            body.append(incoming_main.pop(0)[1])
        body.append(lines[i])
    body += [x[1] for x in incoming_main]

    drop = set()
    old_heads = [i for i, l in enumerate(lines) if ANDROID_HEAD_RE.match(l) and i != tpl["sec"]]
    for h in old_heads:
        drop.update(_section_span(lines, h))
    head_tpl = lines[old_heads[0]] if old_heads else "## Android 0"
    prev_block = []
    if prev_rows:
        prev_block = ["", ANDROID_HEAD_RE.sub(lambda m: m.group(1) + str(latest - 1), head_tpl, count=1), ""] + \
                     header + [x[1] for x in prev_rows] + footer
    out = []
    for i, line in enumerate(lines):
        if i in drop or first <= i <= last:
            if i == first:
                out += body
            if i == last and end == last:
                out += prev_block
            continue
        if tpl["sec"] >= 0 and i == tpl["sec"] and ANDROID_HEAD_RE.match(line):
            line = ANDROID_HEAD_RE.sub(lambda m: m.group(1) + str(latest), line, count=1)
        out.append(line)
        if i == end and end != last:
            out += prev_block
    while out and not out[-1].strip():
        out.pop()
    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


def render_row(tpl, name, url, links):
    """Render a new row in the same style as existing row `tpl`."""
    links_s = ", ".join(links)
    if tpl["style"] == "html":
        line, vals, k = tpl["raw"], [name, url, links_s], [0]
        def sub(m):
            i = k[0]; k[0] += 1
            if i >= 3:
                return m.group(0)
            v = vals[i]
            if i > 0 and v:
                v = ", ".join(f'<a href="{u}">{u}</a>' for u in v.split(", "))
            open_tag = re.match(r"<t[dh][^>]*>", m.group(0), re.I).group(0)
            return f"{open_tag}{v}</{open_tag[1:3]}>"
        return TD_RE.sub(sub, line)
    lead = re.match(r"\s*", tpl["raw"]).group(0)
    row = f"{name} | {url} | {links_s}"
    return lead + (f"| {row} |" if tpl["style"] == "md-outer" else row)


def read_list(path):
    if not path or not os.path.exists(path):
        return set()
    return {l.split("#")[0].strip().lower() for l in open(path) if l.split("#")[0].strip()}


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--readme", default="README.md")
    ap.add_argument("--out", default="rom-scout-out")
    ap.add_argument("--days", type=int, default=60, help="activity window (real commits)")
    ap.add_argument("--discover-days", type=int, default=None, help="search window (default = --days)")
    ap.add_argument("--terms", default="frameworks_base,packages_apps_Settings,frameworks_native,frameworks_opt_telephony")
    ap.add_argument("--deny", default=".github/rom-scout/denylist.txt")
    ap.add_argument("--allow", default=".github/rom-scout/allowlist.txt")
    ap.add_argument("--versions", default=".github/rom-scout/versions.txt", help="'owner version' lines to fix a ROM's Android version")
    ap.add_argument("--cache-hours", type=float, default=20)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0, help="verify at most N new owners (quick test)")
    ap.add_argument("--no-fill-links", action="store_true")
    ap.add_argument("--latest-android", type=int, default=0, help="latest Android version (0 = detect from manifests)")
    ap.add_argument("--latest-min-roms", type=int, default=5, help="ROMs needed on a version before it counts as latest")
    ap.add_argument("--no-remove", action="store_true", help="never remove existing rows")
    ap.add_argument("--max-remove-pct", type=float, default=25, help="skip all removals if more than this %% would go")
    ap.add_argument("--write", action="store_true", help="overwrite --readme with the proposal")
    a = ap.parse_args()

    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    os.makedirs(a.out, exist_ok=True)
    readme = open(a.readme, encoding="utf-8").read()
    rows = parse_readme(readme)
    if len(rows) < 10:
        sample = "\n".join(readme.splitlines()[:15])
        sys.exit(f"Safety stop: only {len(rows)} ROM rows parsed from {a.readme}; refusing to continue.\n"
                 f"First lines of the file:\n{sample}")
    existing = {r["owner"] for r in rows}
    section = {r["owner"]: r["section"] for r in rows}
    deny, allow = read_list(a.deny), read_list(a.allow)
    log(f"{len(rows)} existing ROM rows ({sum(1 for r in rows if r['section'] == 'prev')} in an older-Android "
        f"section), {len(deny)} denylisted, {len(allow)} allowlisted")

    gh = GitHub(get_token())
    disc_days = a.discover_days or a.days
    log(f"Discovering repos pushed in the last {disc_days} days ...")
    found = discover(gh, [t.strip() for t in a.terms.split(",") if t.strip()],
                     now - dt.timedelta(days=disc_days), now)
    log(f"{len(found)} distinct owners from search")

    targets = {k.lower(): (k, v["hits"]) for k, v in found.items()}
    for r in rows:
        targets.setdefault(r["owner"], (r["owner"], {}))
    for x in allow:
        targets.setdefault(x, (x, {}))
    new = [k for k in targets if k not in existing and k not in deny]
    if a.limit:
        keep = set(sorted(new)[:a.limit]) | existing | allow
        targets = {k: v for k, v in targets.items() if k in keep}

    cache_path = os.path.join(a.out, "cache.json")
    cache = json.load(open(cache_path)) if os.path.exists(cache_path) else {}
    results = {}

    def work(key):
        c = cache.get(key)
        if c and c.get("_ver") == CACHE_VER and (time.time() - c["_t"]) < a.cache_hours * 3600:
            return key, c["v"]
        login, hits = targets[key]
        try:
            v = verify_owner(gh, login, list(hits), now, deep=key in existing or key in allow)
        except Exception as e:                       # one bad owner never kills the run
            v = {"login": login, "error": str(e)[:200]}
        return key, v

    log(f"Verifying {len(targets)} owners ...")
    try:
        with cf.ThreadPoolExecutor(a.workers) as ex:
            for n, (key, v) in enumerate(ex.map(work, sorted(targets)), 1):
                results[key] = v
                if not v.get("error"):
                    cache[key] = {"_t": time.time(), "_ver": CACHE_VER, "v": v}
                if n % 25 == 0:
                    log(f"  {n}/{len(targets)}")
                    json.dump(cache, open(cache_path, "w"))
    finally:
        json.dump(cache, open(cache_path, "w"))

    overrides = {}
    if os.path.exists(a.versions):
        for l in open(a.versions):
            parts = l.split("#")[0].split()
            if len(parts) == 2 and parts[1].isdigit():
                overrides[parts[0].lower()] = int(parts[1])
    for key, v in results.items():
        if not v.get("error"):
            choose(v, a.days, now)
            if key in overrides:
                v["androidVersion"], v["versionSource"] = overrides[key], "versions.txt"
    auto, counts = detect_latest(results, a.days, now, a.latest_min_roms)
    latest = a.latest_android or auto
    if not latest:
        sys.exit(f"Could not detect the latest Android version (active ROMs per version: {counts}). "
                 f"Pass --latest-android N.")
    CTX["latest"] = latest
    log(f"Active ROM manifests per Android version: {counts} -> latest = Android {latest}"
        + (" (pinned)" if a.latest_android else " (detected)"))

    for r in sorted(rows, key=lambda r: len(r["owner"])):        # shortest org name = the main ROM
        for f in (family(r["owner"]), family(r["name"])):
            if f:
                CTX["families"].setdefault(f, r["name"])
    for r in sorted(rows, key=lambda r: len(r["owner"])):
        fo = family(r["owner"]) or ""
        for b in (results.get(r["owner"]) or {}).get("vendorBrands", []):
            if fo and (b.startswith(fo) or fo.startswith(b)):
                CTX["brands"][b] = r["owner"]                     # vendor_aospa -> AOSPA, vendor_evolution -> Evolution-X
            else:
                CTX["brands"].setdefault(b, r["owner"])

    names = {r["owner"]: r["name"] for r in rows}
    buckets = {k: [] for k in ("ACCEPT", "REVIEW", "REJECT", "MAIN", "PREV", "KEEP", "REMOVE", "ERROR")}
    for key, v in results.items():
        if key in existing and section.get(key) == "other":
            continue                                             # rows in unrelated tables are left alone
        status, why = classify(v, a.days, now, existing, deny, allow)
        if key in existing and status == "ERROR":
            status = "KEEP"                                      # never move or drop a row on an API error
        v["status"], v["why"], v["listedName"] = status, why, names.get(key)
        buckets[status].append(v)
        if key in existing:
            v["was"] = section.get(key)

    removal_note = ""
    if buckets["REMOVE"] and (a.no_remove or len(buckets["REMOVE"]) > len(rows) * a.max_remove_pct / 100):
        removal_note = (f"**Removals skipped:** {len(buckets['REMOVE'])} rows would be removed "
                        + ("but --no-remove is set." if a.no_remove else
                           f"which is more than {a.max_remove_pct:g}% of the list - check before trusting this run."))
        log(removal_note)
    place = {}
    for v in buckets["MAIN"]:
        place[v["login"].lower()] = "main"
    for v in buckets["PREV"]:
        place[v["login"].lower()] = "prev"
    for v in buckets["KEEP"]:
        place[v["login"].lower()] = "keep"
    for v in buckets["REMOVE"]:
        place[v["login"].lower()] = "keep" if removal_note else "remove"
    for r in rows:
        place.setdefault(r["owner"], "keep")

    fills = {} if a.no_fill_links else {v["login"].lower(): v.get("links", [])
                                        for b in ("MAIN", "PREV", "KEEP") for v in buckets[b]}
    proposed = merge(readme, rows, place, buckets["ACCEPT"], fills, latest)
    if len(parse_readme(proposed)) < len(rows) * 0.5:
        sys.exit("Safety stop: the proposed README would lose more than half of its rows; nothing written.")
    open(os.path.join(a.out, "README.proposed.md"), "w", encoding="utf-8").write(proposed)
    diff = "".join(difflib.unified_diff(readme.splitlines(True), proposed.splitlines(True), "README.md", "README.proposed.md"))
    open(os.path.join(a.out, "README.diff"), "w", encoding="utf-8").write(diff)
    json.dump(results, open(os.path.join(a.out, "candidates.json"), "w"), indent=1, sort_keys=True)

    def tbl(items, cols):
        if not items:
            return "_none_\n"
        s = "| " + " | ".join(c[0] for c in cols) + " |\n|" + "---|" * len(cols) + "\n"
        for v in sorted(items, key=lambda x: x.get("login", "").lower()):
            s += "| " + " | ".join(str(c[1](v)).replace("|", "/") for c in cols) + " |\n"
        return s
    org = ("Org", lambda v: f"[{v['login']}](https://github.com/{v['login']})")
    nm = ("Name", lambda v: v.get("listedName") or v.get("name", ""))
    andr = ("Android", lambda v: f"{v['androidVersion']} ({v.get('versionSource')})" if v.get("androidVersion") else "?")
    mbr = ("Manifest", lambda v: v.get("manifestBranch") or "-")
    lc = ("Last ROM commit", lambda v: (v.get("lastCommit") or "-")[:10]
          + (f" ({v.get('lastCommitRepo')}@{v['lastCommitBranch']}, {v.get('activitySource')})" if v.get("lastCommitBranch") else ""))
    why = ("Why", lambda v: v.get("why", ""))
    own = ("Own manifest projects", lambda v: v.get("manifestOwnProjects", "-"))
    ext = ("Off-GitHub git", lambda v: ", ".join(v.get("externalGit", [])) or "-")
    links = ("Links", lambda v: ", ".join(v.get("links", [])))
    new_cols = [org, nm, andr, mbr, lc, own, ext, why, links]
    ex_cols = [org, nm, andr, mbr, lc, why]
    up = [v for v in buckets["MAIN"] if v.get("was") == "prev"]
    down = [v for v in buckets["PREV"] if v.get("was") == "main"]
    unknown = [v for v in buckets["KEEP"]]
    rej_old = sum(1 for v in buckets["REJECT"] if "older than Android" in v.get("why", ""))
    head = [f"# ROM scout report - {now:%Y-%m-%d %H:%M} UTC\n",
            f"Latest Android: **{latest}** ({'pinned' if a.latest_android else 'detected'}; active ROM manifests "
            f"per version: {', '.join(f'A{k}: {n}' for k, n in counts.items())}). Activity window: {a.days} days. "
            f"Owners checked: {len(results)}.\n",
            f"\nList after this run: **{len(buckets['MAIN']) + sum(1 for v in buckets['ACCEPT'] if v.get('target') == 'main')}** "
            f"on Android {latest}, **{len(buckets['PREV']) + sum(1 for v in buckets['ACCEPT'] if v.get('target') == 'prev')}** "
            f"on Android {latest - 1}.\n" + (f"\n{removal_note}\n" if removal_note else "")]
    changes = [f"\n## New - added to Android {latest} list ({sum(1 for v in buckets['ACCEPT'] if v.get('target') == 'main')})\n",
               tbl([v for v in buckets["ACCEPT"] if v.get("target") == "main"], new_cols),
               f"\n## New - added to Android {latest - 1} list ({sum(1 for v in buckets['ACCEPT'] if v.get('target') == 'prev')})\n",
               tbl([v for v in buckets["ACCEPT"] if v.get("target") == "prev"], new_cols),
               f"\n## Existing - moved down to Android {latest - 1} ({len(down)})\n", tbl(down, ex_cols),
               f"\n## Existing - moved up to Android {latest} ({len(up)})\n", tbl(up, ex_cols),
               f"\n## Existing - {'would be removed (skipped)' if removal_note else 'removed'} ({len(buckets['REMOVE'])})\n",
               tbl(buckets["REMOVE"], ex_cols),
               f"\n## Existing - left where they are ({len(unknown)})\n", tbl(unknown, ex_cols)]
    review = review_sections(buckets["REVIEW"], tbl, new_cols)
    tail = [f"\n## Errors ({len(buckets['ERROR'])})\n",
            tbl(buckets["ERROR"], [("Org", lambda v: v['login']), ("Error", lambda v: v.get('why'))]),
            f"\n_Rejected: {len(buckets['REJECT'])} (of which {rej_old} on Android {latest - 2} or older) - see candidates.json_\n"]
    open(os.path.join(a.out, "report.md"), "w", encoding="utf-8").write("".join(head + changes + review + tail))
    body = "".join(head + changes + review[:2] + tail)                   # PR body: no variant table
    if len(body) > 60000:
        body = body[:60000] + "\n\n_Truncated - full report in the workflow artifact._\n"
    open(os.path.join(a.out, "pr-body.md"), "w", encoding="utf-8").write(body)
    if a.write and diff:
        open(a.readme, "w", encoding="utf-8").write(proposed)

    print(json.dumps({k: len(v) for k, v in buckets.items()}))
    log(f"Done -> {a.out}/report.md, README.proposed.md, README.diff")


if __name__ == "__main__":
    main()
