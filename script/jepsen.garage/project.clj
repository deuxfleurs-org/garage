(defproject jepsen.garage "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Garage"
  :url "https://git.deuxfleurs.fr/Deuxfleurs/garage"
  :license {:name "AGPLv3"
            :url "https://www.gnu.org/licenses/agpl-3.0.en.html"}
  :main jepsen.garage
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.3-SNAPSHOT"]
                 [amazonica "0.3.163"]]
  :repl-options {:init-ns jepsen.garage})
