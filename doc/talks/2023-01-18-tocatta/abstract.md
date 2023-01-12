### (fr) Garage, un système de stockage de données géo-distribué léger et robuste

Garage est un système de stockage de données léger, géo-distribué, qui
implémente le protocole de stockage S3 de Amazon. Garage est destiné
principalement à l'auto-hébergement sur du matériel courant d'occasion. À ce
titre, il doit tolérer un grand nombre de pannes: coupures de courant, coupures
de connexion Internet, pannes de machines, ... Il doit également être facile à
déployer et à maintenir, afin de pouvoir être facilement utilisé par des
amateurs ou des petites organisations.

Cette présentation vous proposera un aperçu de Garage et du choix technique
principal qui rend un système comme Garage possible: le refus d'utiliser des
algorithmes de consensus, remplacés avantageusement par des méthodes à
cohérence faible.  Notre modèle est fortement inspiré de la base de donnée
Dynamo (DeCandia et al, 2007), et fait usage des types de données CRDT (Shapiro
et al, 2011).  Nous exploreront comment ces méthodes s'appliquent à la
construction de l'abstraction "stockage objet" dans un système distribué, et
quelles autres abstractions peuvent ou ne peuvent pas être construites dans ce
modèle.

### (en) Garage, a lightweight and robust geo-distributed data storage system

Garage is a lightweight geo-distributed data store that implements the Amazon
S3 object storage protocol.  Garage is meant primarily for self-hosting at home
on second-hand commodity hardware, meaning it has to tolerate a wide variety of
failure scenarios such as power cuts, Internet disconnections and machine
crashes. It also has to be easy to deploy and maintain, so that hobbyists and
small organizations can use it without trouble. 

This talk will present Garage and the key technical choice that made Garage
possible: refusing to use consensus algorithms and using instead weak
consistency methods, with a model that is loosely based on that of the Dynamo
database (DeCandia et al, 2007) and that makes heavy use of conflict-free
replicated data types (Shapiro et al, 2011).  We will explore how these methods
are suited to building the "object store" abstraction in a distributed system,
and what other abstractions are possible or impossible to build in this model.



