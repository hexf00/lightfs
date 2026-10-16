---- MODULE ProbeIntent ----
EXTENDS VersionCandidate

(* Reachability probe: this invariant is expected to be VIOLATED by TLC
   (a state with intent = "Done" exists). A PASS would mean the gc drain
   is unreachable — a model error. *)
NotIntentDone == intent # "Done"

====================================================================================================
