use std::time::Instant;

use core_types::chunks::GameChunk;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use core_types::helpers::FastSet;

fn hash_to_random(input: i32) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

pub fn test_chunk_borders() {
    let mut chunks = Vec::new();
    let iter_max = 50_000;
    for i in 0..iter_max {
        let x = hash_to_random(i) as i16;
        let y = hash_to_random(i + iter_max) as i16;
        chunks.push(GameChunk {
            x: (x / 64) as i16,
            y: (y / 64) as i16,
        });
    }

    // 2. Test de Performance (Benchmark Temporel)
    let iterations = 10;
    println!("--- BENCHMARK TEMPOREL ---");
    println!("Lancement de {} itérations...", iterations);


    let chunks = chunks.as_slice();

    let start_time = Instant::now();

    for _ in 0..iterations {
        // black_box empêche le compilateur d'optimiser et de supprimer la boucle
        // car il pourrait remarquer qu'on n'utilise pas le résultat ici.
        black_box(GameChunk::get_borders_of(chunks, 1));
    }

    let duration = start_time.elapsed();

    println!("GRID : Temps total     : {:?}", duration);
    println!("Temps moyen/ité : {:?}", duration / iterations);
    println!("--------------------------");

}
fn visualize_map(chunks: &[GameChunk], borders: FastSet<(i16, i16)>) {
    let chunk_set: FastSet<(i16, i16)> = chunks.iter().map(|c| (c.x, c.y)).collect();
    let border_set = borders;

    // Trouver les limites (Bounding Box) pour savoir quelle zone dessiner
    let mut min_x = i16::MAX;
    let mut max_x = i16::MIN;
    let mut min_y = i16::MAX;
    let mut max_y = i16::MIN;

    for &(x, y) in chunk_set.iter().chain(border_set.iter()) {
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }

    println!("--- VISUALISATION SPATIALE ---");
    println!(
        "Légende : 🟦 = Chunk  |  🟥 = Frontière (🟨 == both == problèmes)  |  ⬛ = Vide\n"
    );

    // On dessine de haut en bas, de gauche à droite (avec une marge de 1)
    for y in (min_y - 1)..=(max_y + 1) {
        for x in (min_x - 1)..=(max_x + 1) {
            if chunk_set.contains(&(x, y)) {
                if border_set.contains(&(x, y)) {
                    print!("🟨")
                } else {
                    print!("🟦");
                }
            } else if border_set.contains(&(x, y)) {
                print!("🟥");
            } else {
                print!("⬛");
            }
        }
        println!(); // Retour à la ligne pour la ligne suivante
    }
    println!("------------------------------\n");
}