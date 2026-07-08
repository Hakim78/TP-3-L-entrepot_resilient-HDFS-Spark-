# TP3 : L'entrepot resilient (HDFS et Spark)

Dans ce TP je remplace le stockage local des journaux de commandes d'une plateforme e commerce par un cluster HDFS distribue et tolerant aux pannes. Un cluster Spark standalone, branche sur le meme reseau Docker, lit et ecrit directement dans HDFS sans passer par un volume partage ni par un chemin local.

## Prerequis

1. Docker Desktop avec Docker Compose
2. Python 3 sur la machine hote

## Etape 1 : generation des donnees sur la machine hote

Le script `generation_donnees.py` reprend le code fourni dans le sujet. Il tourne sur ma machine hote, pas dans un conteneur. La seed 7 garantit que les donnees sont reproductibles.

```
python generation_donnees.py
```

J'obtiens trois fichiers de 1000 lignes chacun :

1. `commandes_2026-06-12.csv`
2. `commandes_2026-06-13.csv`
3. `commandes_2026-06-14.csv`

Chaque ligne contient les colonnes id_commande, date, client_id, produit, categorie, quantite, prix_unitaire et entrepot. Ces fichiers ne sont pas versionnes dans le git puisqu'ils se regenerent a l'identique avec le script.

## Etape 2 : cluster HDFS avec Docker Compose

Le fichier `docker-compose.yml` decrit le cluster de stockage. Il contient un namenode qui gere les metadonnees du systeme de fichiers et trois datanodes qui stockent physiquement les blocs. Tous les conteneurs sont relies au reseau bridge `entrepot_net`, celui sur lequel je brancherai aussi le cluster Spark. J'utilise les images `bde2020/hadoop` en version Hadoop 3.2.1 car elles se configurent entierement par variables d'environnement.

Toute la configuration Hadoop est centralisee dans `hadoop.env`. Les points importants :

1. `fs.defaultFS` vaut `hdfs://namenode:9000`, c'est l'adresse que les clients HDFS et Spark utiliseront a l'interieur du reseau Docker.
2. `dfs.replication` vaut 3, chaque bloc est donc copie sur les trois datanodes, ce qui repond a l'exigence de tolerance aux pannes du sujet.
3. `dfs.blocksize` est abaisse a 32 Ko. La taille par defaut est de 128 Mo alors que mes fichiers font environ 76 Ko, chaque fichier tiendrait donc dans un seul bloc et je ne pourrais pas montrer de vrai decoupage. Avec 32 Ko chaque CSV se decoupe en 3 blocs. Je dois aussi abaisser `dfs.namenode.fs-limits.min-block-size` car HDFS refuse par defaut une taille de bloc sous 1 Mo.
4. Les options `dfs.client.use.datanode.hostname` et `dfs.datanode.use.datanode.hostname` forcent l'usage des noms de conteneurs plutot que des adresses IP internes, ce qui evite les problemes de resolution quand un client contacte directement un datanode.

Seul le port 9870 de l'interface web du namenode est publie sur la machine hote. Je ne publie pas le port RPC 9000 car il etait deja occupe sur ma machine et il ne sert qu'aux echanges internes entre conteneurs, Spark y accedera par le reseau Docker.

Demarrage du cluster :

```
docker compose up -d
docker compose ps
```

Verification que les trois datanodes sont bien enregistres aupres du namenode :

```
docker exec namenode hdfs dfsadmin -report
```

Le rapport affiche `Live datanodes (3)` avec datanode1, datanode2 et datanode3. Je verifie aussi que ma configuration est prise en compte :

```
docker exec namenode hdfs getconf -confKey dfs.replication
docker exec namenode hdfs getconf -confKey dfs.blocksize
docker exec namenode hdfs getconf -confKey fs.defaultFS
```

J'obtiens 3, 32768 et hdfs://namenode:9000. L'interface web du namenode est accessible sur http://localhost:9870 et l'onglet Datanodes montre les trois noeuds vivants.

## Etape 3 : chargement des fichiers dans HDFS

Les CSV sont sur ma machine hote et le sujet interdit le simple partage par volume. Je copie donc les fichiers dans le conteneur du namenode avec docker cp puis je les charge dans HDFS avec la commande hdfs dfs. C'est cette deuxieme commande qui fait le vrai travail : le client HDFS decoupe chaque fichier en blocs et le namenode orchestre leur replication sur les datanodes.

```
docker cp commandes_2026-06-12.csv namenode:/tmp/
docker cp commandes_2026-06-13.csv namenode:/tmp/
docker cp commandes_2026-06-14.csv namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /data/commandes
docker exec namenode sh -c "hdfs dfs -put -f /tmp/commandes_*.csv /data/commandes/"
docker exec namenode hdfs dfs -ls /data/commandes
```

Le listing confirme la presence des trois fichiers avec un facteur de replication de 3 :

```
Found 3 items
-rw-r--r--   3 root supergroup      76353 2026-07-08 13:07 /data/commandes/commandes_2026-06-12.csv
-rw-r--r--   3 root supergroup      76582 2026-07-08 13:07 /data/commandes/commandes_2026-06-13.csv
-rw-r--r--   3 root supergroup      76371 2026-07-08 13:07 /data/commandes/commandes_2026-06-14.csv
```

Pour prouver le decoupage en blocs et leur repartition je lance fsck :

```
docker exec namenode hdfs fsck /data/commandes -files -blocks -locations
```

La sortie complete est archivee dans `preuves/etape3_fsck_apres_chargement.txt`. Ce qu'elle montre :

1. Chaque fichier de 76 Ko est decoupe en 3 blocs, deux blocs pleins de 32768 octets et un dernier bloc plus petit qui porte le reste.
2. Chaque bloc affiche Live_repl=3 avec les adresses des trois datanodes qui en detiennent une copie.
3. Le resume global annonce 9 blocs valides, une replication moyenne de 3.0, aucun bloc manquant ni sous replique, et le statut HEALTHY.

Extrait pour le premier fichier :

```
/data/commandes/commandes_2026-06-12.csv 76353 bytes, replicated: replication=3, 3 block(s):  OK
0. blk_1073741825_1001 len=32768 Live_repl=3 [172.26.0.3:9866, 172.26.0.4:9866, 172.26.0.2:9866]
1. blk_1073741826_1002 len=32768 Live_repl=3 [172.26.0.4:9866, 172.26.0.3:9866, 172.26.0.2:9866]
2. blk_1073741827_1003 len=10817 Live_repl=3 [172.26.0.3:9866, 172.26.0.4:9866, 172.26.0.2:9866]
```

Chaque bloc existe donc bien sur les trois datanodes a la fois, la perte d'un noeud ne fait perdre aucune donnee.
