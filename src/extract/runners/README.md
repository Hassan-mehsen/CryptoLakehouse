# runners/

Ce dossier contient des scripts de test local pour chaque extracteur.

Chaque script peut être exécuté manuellement pour tester un endpoint spécifique,
en dehors d'Airflow. Cela permet de valider le comportement individuel des classes
avant leur intégration dans les DAGs.

Les fichiers `.py` de test sont ignorés dans le `.gitignore` pour ne pas encombrer le dépôt.



/----------------------------------------------------


# runners/

Ce dossier contient des **scripts de test local** pour les extracteurs de données.

Chaque fichier ici permet de tester individuellement un extracteur (par endpoint)
sans passer par Airflow. Cela facilite le développement, le debug, et la validation
des appels API en local.

## Structure

- `__init__.py` : permet de rendre ce dossier importable comme un package
- `example_test.py` *(non versionné)* : fichier typique de test local
- `README.md` : ce fichier

Les fichiers `.py` sont ignorés du dépôt Git via `.gitignore`, pour ne pas encombrer
le repository avec du code temporaire ou non exécutable en production.

> Tu peux ajouter tes propres tests ici librement pour développer plus efficacement.

