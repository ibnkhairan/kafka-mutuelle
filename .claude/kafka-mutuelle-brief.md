# Projet kafka-mutuelle : brief de cadrage

> À donner à Claude CLI comme contexte de départ (ou à renommer `CLAUDE.md` à la racine du dépôt).
> Statut : **conception validée, aucun code écrit**. Phase 0 (Kafka en ligne de commande) en cours.

## 1. Objectif

Monter en compétence sur **Kafka avec Java / Spring** pour répondre à des questions d'entretien, en pouvant **voir en live ce qui se passe** (messages, partitions, offsets, lag, rebalancing).

Niveau de départ : junior sur Kafka (comprend le concept). Pas de contrainte de temps : on avance une techno à la fois, en comprenant chaque brique.

## 2. Consignes de travail pour Claude

- Réponses en **français**, techniques et directes. Solutions **minimales et propres**.
- **Diagnostic et données réelles avant recommandations.** Vérifier les affirmations dans la documentation officielle (versions, commandes) plutôt que de raisonner de mémoire.
- **Ne rien développer tant que l'étape n'est pas validée.** Expliquer d'abord, coder ensuite.
- **Clean code** et architecture hexagonale légère (voir §5).
- **Tests obligatoires** : JUnit 5, Mockito, AssertJ pour l'unitaire ; `@EmbeddedKafka` et Testcontainers pour l'intégration. Mockito ne prouve rien sur le comportement réel de Kafka : les points Kafka (retry, DLT, sérialisation, routage par clé) se prouvent par des tests d'intégration.
- Chaque expérience Kafka doit produire une **preuve** (test ou capture) réutilisable en entretien.

## 3. Environnement

- Windows, IntelliJ IDEA, **Maven**, Java 17 (aligné sur la mission), Docker Desktop.
- Dépôt Git unique, multi-modules Maven.
- Utiliser **PowerShell ou cmd** pour les commandes Docker (Git Bash réécrit les chemins `/opt/...`; sinon préfixer par `MSYS_NO_PATHCONV=1`, et `winpty` devant `docker` si `-ti` pose problème).
- Docker Desktop doit être démarré avant toute commande `docker`.

## 4. Domaine métier et flux

Inspiré de la mission actuelle (mutuelles : cotisations, prélèvements, impayés).

```
cotisation-service ──► cotisations.emises ──► prelevement-service ──► prelevements.resultats ──► impaye-service
                                                                                        └──► kafka-lab (dashboard live)
```

| Service | Rôle | Kafka |
|---|---|---|
| `cotisation-service` | API REST qui émet des cotisations, plus un générateur de rafales | Producer |
| `prelevement-service` | Simule le prélèvement SEPA (accepté ou rejeté selon une règle **déterministe**, pour des tests reproductibles) | Consumer puis producer |
| `impaye-service` | Crée l'impayé sur un rejet, déclenche une relance après N rejets | Consumer, producer de `impayes.relances` |
| `kafka-lab` | Dashboard live (SSE ou WebSocket) | Consumer indépendant (groupe propre) lisant tous les topics |

### Topics

| Topic | Clé | Partitions | Remarque |
|---|---|---|---|
| `cotisations.emises` | `idAdherent` | 3 | Ordre garanti par adhérent |
| `prelevements.resultats` | `idAdherent` | 3 | **Un seul topic** avec un champ `type` (ACCEPTE / REJETE) pour préserver l'ordre entre événements d'une même clé. À démontrer par un test. |
| `impayes.relances` | `idAdherent` | 3 | Alimente une éventuelle notification |

Les topics de retry et Dead Letter Topic seront ajoutés en phase 3.

## 5. Structure et architecture

### Maven multi-modules

```
kafka-mutuelle/
├── pom.xml                  (parent, Java 17)
├── events-contract/         records d'événements partagés, sans dépendance Spring
├── cotisation-service/
├── prelevement-service/
├── impaye-service/
├── kafka-lab/
├── infra/docker-compose.yml
├── docs/                    journal d'apprentissage par phase
└── README.md
```

### Architecture hexagonale légère (par service)

```
domain/        métier pur : aucun import Spring ni Kafka
application/   cas d'usage + ports (interfaces)
adapter/in/    listeners Kafka, contrôleurs REST
adapter/out/   publishers Kafka, persistance
config/
```

Règle : **les dépendances pointent vers le centre** (un adaptateur connaît l'application, jamais l'inverse). Les tests du `domain` et de `application` tournent sans broker ni contexte Spring. Événements immuables (`record`), DTO séparés des objets du domaine, exceptions métier distinctes des erreurs techniques, configuration Kafka externalisée.

### Persistance

- H2 au départ (table des événements déjà traités, pour l'idempotence).
- Oracle en phase 6 uniquement, pour l'outbox.

## 6. Plan par phases

| Phase | Contenu | Doit permettre d'expliquer |
|---|---|---|
| 0 | Kafka en CLI (Docker) + Kafka UI | topic, partition, offset, consumer group |
| 1 | Producer et consumer Spring, tests unitaires | `KafkaTemplate`, `@KafkaListener`, commit des offsets |
| 2 | Clés, rebalancing, plusieurs instances | ordre, parallélisme, rebalancing |
| 3 | Idempotence, retries, `DefaultErrorHandler`, `@RetryableTopic`, Dead Letter Topic | at-least-once, gestion d'erreurs |
| 4 | **Prometheus puis Grafana** (métriques applis via Actuator/Micrometer, lag via un exporter Kafka à choisir dans la doc) | lag, supervision |
| 5 | Dashboard live `kafka-lab` + endpoints de scénarios (rafale, message empoisonné, doublon, arrêt d'un consumer) | démo visuelle |
| 6 | Outbox Oracle, Avro et Schema Registry, transactions et exactly-once, Kafka Streams | sujets avancés |
| 7 | Tests d'intégration complets (EmbeddedKafka, Testcontainers), puis éventuellement ELK | prouver ce qui précède |

Trois niveaux de « live » à distinguer : Kafka UI (contenu), dashboard maison (parcours d'un message), Prometheus/Grafana (tendances et lag).

## 7. Décisions déjà validées

1. Un seul dépôt multi-modules Maven.
2. Un topic de résultats avec champ `type` (pas deux topics).
3. Phase 0 en ligne de commande avant tout Spring.
4. Domaine mutuelle (cotisations / prélèvements / impayés).
5. Prometheus/Grafana inclus (technos nouvelles voulues).
6. Clean code + tests JUnit/Mockito dès le début.

## 8. Phase 0 : commandes

Vérifiées dans le guide Docker officiel (https://docs.docker.com/guides/kafka/) :

```bash
# Démarrer Kafka (KRaft, sans ZooKeeper, aucun téléchargement manuel)
docker run -d --name=kafka -p 9092:9092 apache/kafka

# Producer console (terminal A) : une ligne = un message
docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server :9092 --topic demo

# Consumer console (terminal B)
docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server :9092 --topic demo --from-beginning
```

**À vérifier sur la machine** (dans le même dossier `/opt/kafka/bin/`, non confirmé par la page consultée ; contrôler avec `docker exec kafka ls /opt/kafka/bin` et `--help`) :

- `kafka-topics.sh` : créer `cotisations.emises` avec 3 partitions (un topic auto-créé n'a que la valeur par défaut).
- `kafka-consumer-groups.sh` : lister les groupes, voir le lag.
- Propriétés du producer/consumer pour clé, partition, offset : `parse.key`, `key.separator`, `print.key`, `print.partition`, `print.offset`.

### Expériences de la phase 0

1. Créer `cotisations.emises` (3 partitions).
2. Produire des messages **avec clé** ; observer la partition et l'offset à la lecture.
3. Vérifier que la même clé va toujours dans la même partition.
4. Lancer **2 consumers dans le même groupe** ; observer la répartition des partitions.
5. Arrêter l'un des deux ; observer la reprise de ses partitions (rebalancing).
6. Interroger le groupe pour voir le lag.

Noter pour chaque expérience : commande, résultat observé, enseignement (fichier `docs/phase-0-cli.md`).

## 9. Prochaines étapes

1. Exécuter la phase 0 et consigner les observations dans `docs/phase-0-cli.md`.
2. Créer le dépôt Git : `README.md` (objectif, schéma du flux, prérequis Docker/Java 17/Maven, avancement par phase), `.gitignore` (`target/`, `.idea/`, `*.iml`), `docs/`.
3. **Vérifier dans la documentation officielle** les versions compatibles de Spring Boot, spring-kafka et Kafka, et **figer le tag de l'image Docker** (pas `latest`).
4. Écrire `infra/docker-compose.yml` (Kafka KRaft + Kafka UI).
5. Démarrer la phase 1 : créer le parent Maven et `events-contract`, puis `cotisation-service` avec ses tests unitaires.

## 10. Points ouverts

- Choix de l'exporter de lag Kafka pour Prometheus (kafka-exporter ou JMX exporter) : à trancher dans la doc en phase 4.
- Image `apache/kafka` (JVM) ou `apache/kafka-native` (démarrage plus rapide, moins de mémoire) : à choisir au moment du `docker-compose.yml`.
