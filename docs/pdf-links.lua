-- Réécrit les liens relatifs des markdown vers le repository GitHub,
-- pour que les PDF partagés ne contiennent aucun chemin local (file://)
-- et que leurs liens fonctionnent pour tout lecteur.
local REPO = "https://github.com/elhajjaji/dbt-benefit/blob/main/"

function Link(el)
  local t = el.target
  if not (t:match("^%a+://") or t:match("^#") or t:match("^mailto:")) then
    el.target = REPO .. t
  end
  return el
end
