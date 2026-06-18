use std::time::Instant;

use core_types::chunks::{get_chunk_size, GameChunk};
use core_types::helpers::FastSet;
use core_types::Rect;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;

fn hash_to_random(input: i32) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

pub fn test_math() {
    let world_size = 800.0;
    let tree_depth = 1;
    let chunk_size = get_chunk_size(world_size, tree_depth);
    let rect = Rect {
        min_x: -world_size / 2.0,
        min_y: -world_size / 2.0,
        max_x: world_size / 2.0,
        max_y: world_size / 2.0,
    };
    println!("== TEST CHUNKS ==");

    println!(
        "{:?} -> {:?}\n\nand splits to\n",
        rect,
        rect.bounding_chunk_aera(chunk_size)
    );

    for mini_rect in rect.split() {
        let as_chunk = mini_rect.bounding_chunk_aera(chunk_size);
        println!("{:?} -> {:?}", mini_rect, as_chunk);
        for mini_mini_rect in mini_rect.split() {
            let as_chunk = mini_mini_rect.bounding_chunk_aera(chunk_size);
            println!("\t\t{:?} -> {:?}", mini_mini_rect, as_chunk);
        }
    }
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
fn _visualize_map(chunks: &[GameChunk], borders: FastSet<(i16, i16)>) {
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
    println!("Légende : 🟦 = Chunk  |  🟥 = Frontière (🟨 == both == problèmes)  |  ⬛ = Vide\n");

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
