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
