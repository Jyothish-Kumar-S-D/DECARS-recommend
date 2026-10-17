import json
import random
from faker import Faker

fake = Faker()

# Define some categories and matching emojis to keep it realistic
CATEGORIES = {
    "Electronics": ["🎧", "📺", "📻", "🔋", "🔌"],
    "Computers": ["💻", "🖥️", "⌨️", "🖱️", "💾"],
    "Apparel": ["👕", "👖", "👗", "🧥", "👟"],
    "Home": ["🛋️", "🛏️", "🪑", "🚪", "🪞"],
    "Kitchen": ["🍳", "🍲", "🔪", "🍽️", "🥤"],
    "Toys": ["🧸", "🧱", "🪀", "🧩", "🎲"],
    "Outdoors": ["⛺", "🚴", "🧗", "🔥", "🛶"]
}

def generate_products(num_items):
    print(f"Generating {num_items} products... Please wait.")
    products = []
    
    for i in range(1, num_items + 1):
        category = random.choice(list(CATEGORIES.keys()))
        
        # Build a realistic looking product name
        brand = fake.company()
        product_type = fake.word().capitalize()
        name = f"{brand} {product_type}"
        
        product = {
            "id": i,
            "name": name,
            "category": category,
            "price": round(random.uniform(10.0, 1500.0), 2), # Random price between $10 and $1500
            "img": random.choice(CATEGORIES[category]),
            "rating": round(random.uniform(3.0, 5.0), 1)     # Random rating between 3.0 and 5.0
        }
        products.append(product)
        
    return products

# Generate 100,000 products
large_dataset = generate_products(1000000)

# Save to a JSON file
with open("products.json", "w", encoding="utf-8") as file:
    json.dump(large_dataset, file, indent=4, ensure_ascii=False)

print("Success! 100,000 products have been saved to 'products.json'")