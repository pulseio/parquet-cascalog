(ns pail-test.core
  (:import backtype.hadoop.pail.PailStructure))

(deftype FooPail []
  PailStructure
  (isValidTarget [this strings] true)
  (getTarget [this o] [(first o) (second o)])
  (deserialize [this o] (vec (.split (String. o) "\001")))
  (serialize [this o] (.getBytes (clojure.string/join "\001" (map str (drop 2 o)))))
  (getType [this] clojure.lang.PersistentVector))

(comment (use 'cascalog.api)
         (def foo-pail (pail_test.core.FooPail.))
         
         (def my-tap (backtype.cascading.tap.PailTap.
                      "/tmp/stuff"
                      (backtype.cascading.tap.PailTap$PailTapOptions.
                       (backtype.cascading.tap.PailTap/makeSpec nil foo-pail)
                       "bytes" nil (backtype.hadoop.pail.AllPailPathLister.))))

         

         (?<- my-tap
              [?out]
              ([[["a=foo" "b=bar" 0 1]]
                [["a=foo" "b=bar" 0 1]]
                [["a=foo" "b=smegma" 0 10]]] ?out)))
