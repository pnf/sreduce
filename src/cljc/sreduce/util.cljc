(ns sreduce.util
  (:require
   #?@(:clj  [[clojure.core.async :as async :refer [<! >! timeout chan promise-chan pipe onto-chan go go-loop alts!]]]
       :cljs [[cljs.core.async :as async :refer [<! >! timeout chan promise-chan pipe onto-chan]]]))
  #?(:cljs (:require-macros
            [cljs.core.async.macros :as m :refer [go go-loop]])))


(defn launch-reductions [c-redn f l ps]
  "Take 
     credn - a channel onto which to place reductions in the format
             [queue-number value  volatile-destination], 
     f      - a function f that reduces two values and returns
              a channel that will return the reduced value,
     l      - the index of the queue from which came the ...
     ps     - pairs of values to be reduced.
   Returns the sequence of volatile destinations."
  (let [pairs (take-while (fn [[a b]] (and a b))
                          (partition 2 (map deref ps)))]
    (map (fn [[a b]]
           (let [v (volatile! nil)]
             (go (>! c-redn [(inc l) (<! (f a b)) v]))
             v)) pairs)))

(defn wrapv
  "Return a channel that will transform its inputs by wrapping them in the canonical
  reduction format for a raw input - queue number 0 and no volatile destination."
  [] (chan 1 (map (fn [x] [0 x nil]))))

(defn pretty-state [{:keys [queues np n]}]
  (let [queues  (map (fn [l] (let [psd (map deref  (or (queues l) []))]
                             [(take-while identity (take 2 psd)) (count psd) (count (filter not psd))]
                             ))
                    (range 10))]
    {:n n :np np :queues queues}))

(defn assoc-reduce [f c-in & {:keys [np-max debug n] :or {np-max 5 debug false}}]
  (let [c-result (promise-chan)
        c-redn    (chan np-max)]
    (go-loop [{:keys [c-in queues np] :as state} {:c-in (pipe c-in (wrapv)) :queues {} :np 0 :i 0 :n n}]
      ;;(prn (pretty-state state))
      (if debug (>! debug (pretty-state state)))
      ;; Construct list of channels to listen on:
      (if-let [cs (seq (filter identity
                               (list
                                (if (pos? np) c-redn) ; reduction channel if we're expecting any
                                (if (< np np-max) c-in))))] ; input channel if we want any
        (let [[[l res v]  c]  (alts! cs)]
          ;; The only condition under which we get a nil here is  when we've run out
          ;; of inputs, ...
          (if-not l 
            (recur (assoc state :c-in nil)) ; so mark the input channel closed.
            (let [q (if v ; v will be non-nil only if this is a reduction, ...
                      ;; so store the result in v, and return the queue it came from;
                      (do (vreset! v res) (queues l))
                      ;; otherwise, this is an input, so add it to the zeroeth queue.
                      (concat (queues 0) [(volatile! res)])) 
                  vs  (launch-reductions c-redn f l q)
                  nr  (count vs)
                  q   (drop (* 2 nr) q ) ;; Remove the inputs for these new reductions.
                  np  (cond-> (+ np nr) ;; Add new reductions to parallelism count, 
                        (pos? l) dec) ;; and subtract the one we just completed.
                  n   (cond-> (:n state) (zero? l) (dec)) ;; decrement the remaining inputs.
                  l2  (inc l)
                  q2 (concat (queues l2) vs) ;; Add reduction placeholders to next queue
                  ]
              ;; And repeat, with the new counts and queues.
              (recur (assoc state :n n :np np :queues (assoc queues l q l2 q2))))))
        ;; Both channels closed, so we're done.  Assemble a sequence of accumulated reductions.
        (let [reds (->>
                    (seq queues)    ;; Sort queues by queue number
                    (sort-by first)
                    (map second)
                    (map first)     ;; Take head of each non-empty queue
                    (filter identity)
                    (map deref)     ;; Extract the actual values.
                    reverse
                    )]
          (if (> (count reds) 1)
            ;; Create a new input channel and dump the reductions onto it.
            (let [c-in (wrapv)]
              (onto-chan c-in reds)
              (recur {:n (count reds) :c-in c-in :queues {} :np 0}))
            ;; Only one left, so we're done.
            (do
              (when debug (async/close! debug))
              (>! c-result (first reds)))))
        ))
    c-result))


(defn delay-spool [as t do-rand]
  (let [c (chan)]
    (go-loop [[a & as] as]
      (if a
        (do (>! c a)
            (<! (timeout (if do-rand (rand-int t) t)))
            (recur as))
        (async/close! c)))
    c))

(defn plusso [t do-rand]
  (fn [a b] (go (<! (timeout (if do-rand (rand-int t) t))) (+ a b))))
