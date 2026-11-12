# Phase 0 : Kafka en ligne de commande

Environnement : Docker Desktop, image `apache/kafka:4.4.0` (Kafka 4.4.0, mode KRaft, un seul broker).
Source du tag : https://kafka.apache.org/community/downloads/ (le guide https://docs.docker.com/guides/kafka/ n'utilise pas de tag, donc `latest`).
Les commandes sont tapées dans PowerShell.

Pour chaque expérience : commande, résultat observé, enseignement.

## 0. Démarrer Kafka
- **Commande** :
  ```powershell
  docker run -d --name=kafka -p 9092:9092 apache/kafka:4.4.0
  docker ps
  docker logs kafka --tail 20
  ```
- **Résultat observé** : conteneur `Up`, port 9092 exposé. Les logs affichent `Kafka version: 4.4.0` et `Kafka Server started` (`KafkaRaftServer nodeId=1`).
- **Enseignement** : le tag `4.4.0` existe et se télécharge ; pas de ZooKeeper (KRaft).

## 1. Créer `cotisations.emises` (3 partitions)
- **Commande** :
  ```powershell
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cotisations.emises --partitions 3 --replication-factor 1
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cotisations.emises
  ```
- **Résultat observé** : `Created topic cotisations.emises.` puis `PartitionCount: 3` et trois partitions (0, 1, 2) avec `Leader: 1`, `Isr: 1`. Un avertissement signale que mélanger `.` et `_` dans les noms de topics peut provoquer des collisions de métriques ; nos noms n'utilisent que le point.
- **Enseignement** : un topic auto-créé n'aurait que la valeur par défaut ; il faut le créer explicitement pour avoir 3 partitions. Facteur de réplication à 1 car un seul broker.

## 2. Produire avec clé, lire partition et offset
- **Commande** :
  ```powershell
  # Consumer
  docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cotisations.emises --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
  # Producer (une ligne `cle:valeur` = un message)
  docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cotisations.emises --reader-property parse.key=true --reader-property key.separator=:
  ```
- **Résultat observé** : `adh1` et `adh2` en partition 2, `adh3` en partition 0. Premier lot : partition 2 offsets 0 à 4, partition 0 offset 0.
- **Enseignement** : l'offset est la position du message **dans sa partition** ; chaque partition a ses propres offsets. Deux clés différentes peuvent partager une partition.
- **Note** : l'option `--property` est dépréciée ; utiliser `--formatter-property` (consumer) et `--reader-property` (producer), d'après l'avertissement affiché par les outils.

## 3. Même clé ⇒ même partition
- **Commande** : même producer, deuxième lot avec les clés `adh4`, `adh5`, `adh6`, puis `adh1`, `adh3`, `adh2`.
- **Résultat observé** : `adh1` retombe en partition 2 (offset 5), `adh2` en partition 2 (offset 6), `adh3` en partition 0 (offset 3, car `adh4` et `adh6` y sont arrivés avant). `adh5` peuple la partition 1.
- **Enseignement** : le routage d'une clé est stable. L'ordre n'est garanti qu'à l'intérieur d'une partition (les messages de `adh1` sont lus dans l'ordre d'écriture). Un offset dépend de l'ordre d'arrivée dans la partition, pas de la clé.

## 4. Deux consumers dans le même groupe
- **Commande** : dans deux terminaux, le consumer avec `--group groupe-demo` (sans `--from-beginning`), puis un lot `adh1`, `adh3`, `adh5` ×2 ; ensuite :
  ```powershell
  docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group groupe-demo
  ```
- **Résultat observé** : un consumer lit les partitions 0 et 1 (`adh3`, `adh5`), l'autre la partition 2 (`adh1`). Aucun message n'est reçu par les deux. Lag à 0 partout.
- **Enseignement** : dans un groupe, chaque partition est lue par un seul consumer ; 3 partitions entre 2 consumers donnent une répartition 2 + 1. `CURRENT-OFFSET` est l'offset du **prochain** message à lire.

## 5. Arrêt d'un consumer : rebalancing
- **Commande** : `Ctrl+C` sur le consumer de la partition 2, puis `--describe`, puis production de `adh1:g`, `adh3:h`, `adh5:i`.
- **Résultat observé** : les partitions 0, 1 et 2 sont attribuées au consumer restant. `adh1:g` est lu en partition 2 à l'offset 9, là où le groupe s'était arrêté.
- **Enseignement** : à l'arrêt d'un consumer, Kafka redistribue ses partitions aux autres (rebalancing). Les offsets sont conservés côté Kafka, donc la reprise se fait sans perte ni relecture.

## 6. Lag du groupe
- **Commande** : arrêt du consumer, production de 5 messages (`adh1` ×2, `adh3`, `adh5` ×2), puis `--describe` ; relance du consumer du groupe, puis `--describe`.
- **Résultat observé** : `Consumer group 'groupe-demo' has no active members.` ; lag de 1, 2 et 2 (`CURRENT-OFFSET` 7, 4, 10 pour `LOG-END-OFFSET` 8, 6, 12). Après relance du consumer, lag à 0 partout. Les offsets de fin finaux (9, 8, 14) montrent qu'un second lot de 5 messages a été produit avant le rattrapage ; l'affichage du consumer relancé n'a pas été conservé.
- **Enseignement** : lag = `LOG-END-OFFSET` − `CURRENT-OFFSET`. Un groupe sans consumer actif garde ses offsets, et rattrape son retard au redémarrage.
