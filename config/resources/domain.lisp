(in-package :mu-cl-resources)

;; reading in the domain.json
(read-domain-file "domain.json")
(read-domain-file "dataset.json")
(read-domain-file "task.lisp")
(read-domain-file "shacl.json")

(define-resource file ()
  :class (s-prefix "nfo:FileDataObject")
  :properties `((:name :string ,(s-prefix "nfo:fileName"))
                (:format :string ,(s-prefix "dct:format"))
                (:size :number ,(s-prefix "nfo:fileSize"))
                (:extension :string ,(s-prefix "dbpedia:fileExtension"))
                (:created :datetime ,(s-prefix "nfo:fileCreated")))
  :has-one `((file :via ,(s-prefix "nie:dataSource")
                   :inverse t
                   :as "download"))
  :resource-base (s-url "http://example-resource.com/files/")
  :features `(include-uri)
  :on-path "files")
