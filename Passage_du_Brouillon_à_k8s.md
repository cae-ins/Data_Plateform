Pour passer du brouillon à une exécution via le Spark Operator, il y a un principe une strcuture technique à respecter : on sépare le code (logique métier) de la configuration (infrastructure).

Voici comment t'organiser en 3 étapes :

Nettoyer le script Python (staging_bronze.py)
Dans Kubernetes, c'est le Spark Operator qui gère l'allocation de la mémoire et les paramètres réseau. On doit donc alléger le script pour qu'il soit portable.

Ce qu'il faut enlever du script :

spark.driver.host et bindAddress (Kubernetes s'en occupe).

spark.driver.memory (On le définit dans le YAML).

spark.jars.packages (On les définit dans le YAML pour que l'opérateur les télécharge avant le lancement).


NB : l'exemple du passage au brouillon à l'execution avec spark-operator s'appuie sur le fichier "staging-bronze.ipynb", on pourra donc l'adapté en fonction du cas d'usage.

