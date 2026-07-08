# je reprends le script fourni dans le sujet, il tourne sur ma machine hote et pas dans un conteneur
# la seed est fixee pour que tout le monde retrouve exactement les memes donnees
import csv, random
random.seed(7)

produits = {
    "Casque audio": ("Electronique", 45, 120),
    "Livre": ("Culture", 8, 25),
    "Chaise bureau": ("Mobilier", 60, 250),
    "Plante verte": ("Maison", 12, 40),
    "Console jeux": ("Electronique", 200, 450),
}
entrepots = ["Entrepot Nord", "Entrepot Sud", "Entrepot Est"]
dates = ["2026-06-12", "2026-06-13", "2026-06-14"]

for date in dates:
    rows = []
    for i in range(1, 1001):
        produit = random.choice(list(produits.keys()))
        categorie, lo, hi = produits[produit]
        quantite = random.randint(1, 5)
        prix = round(random.uniform(lo, hi), 2)
        entrepot = random.choices(entrepots, weights=[0.5, 0.3, 0.2])[0]
        rows.append([f"C{date.replace('-','')}{i:04d}", date, f"CL{random.randint(1,400):04d}",
                     produit, categorie, quantite, prix, entrepot])
    with open(f"commandes_{date}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id_commande","date","client_id","produit","categorie","quantite","prix_unitaire","entrepot"])
        w.writerows(rows)
