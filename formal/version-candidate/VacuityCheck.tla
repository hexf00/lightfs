---- MODULE VacuityCheck ----
EXTENDS VersionCandidate

(* Expected-failure vacuity probe (S4.12.2 protocol): the negation of a
   passing liveness property, stated as a PURE temporal formula so TLC
   cannot optimize it into an invariant path. TLC must report this
   PROPERTY as VIOLATED — evidence the FairSpec behavior set is nonempty
   AND the liveness obligations have real content. (Reachability of
   intent = "Done" and live = TRUE was separately confirmed by invariant
   probes on Spec.) *)
NegIntentCompletes == ~<>(intent = "Done")

NegCandidateResolves == ~<>[](cand # "Tracked")

====================================================================================================
