---- MODULE ProbeLive ----
EXTENDS VersionCandidate

(* Reachability probe: this invariant is expected to be VIOLATED by TLC
   (a state with live = TRUE exists — the CAS path is exercised). *)
NotLive == ~live

====================================================================================================
