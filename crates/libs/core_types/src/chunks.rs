use crate::Rect;
use bitcode::{Decode, Encode};
use std::vec;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash, Copy)]
pub struct GameChunk {
    pub x: i16,
    pub y: i16,
}

impl GameChunk {
    pub fn to_core_rect(&self, size: f32) -> Rect {
        let min_x = self.x as f32 * size;
        let min_y = self.y as f32 * size;
        Rect {
            min_x,
            max_x: min_x + size,
            min_y,
            max_y: min_y + size,
        }
    }

    pub fn get_bounding_area(chunks: &[GameChunk]) -> GameChunkAera {
        let mut bounding_aera = GameChunkAera {
            x_min: i16::MAX,
            x_max: i16::MIN,
            y_min: i16::MAX,
            y_max: i16::MIN,
        };

        for c in chunks {
            bounding_aera.x_min = bounding_aera.x_min.min(c.x);
            bounding_aera.x_max = bounding_aera.x_max.max(c.x);
            bounding_aera.y_min = bounding_aera.y_min.min(c.y);
            bounding_aera.y_max = bounding_aera.y_max.max(c.y);
        }

        bounding_aera
    }

    pub fn get_borders_of(chunks: &[GameChunk], margin: u8) -> Vec<GameChunk> {
        if chunks.is_empty() || margin == 0 {
            return Vec::new();
        }

        // 1. Trouver la "Bounding Box" (limites) de tes chunks
        let mut min_x = i16::MAX;
        let mut max_x = i16::MIN;
        let mut min_y = i16::MAX;
        let mut max_y = i16::MIN;

        for c in chunks {
            min_x = min_x.min(c.x);
            max_x = max_x.max(c.x);
            min_y = min_y.min(c.y);
            max_y = max_y.max(c.y);
        }

        // 2. Créer une grille avec une marge de 1 pour les bordures
        // On convertit en `usize` pour les index de tableau
        let width = (max_x - min_x + 3) as usize;
        let height = (max_y - min_y + 3) as usize;

        // Un Vec<bool> de cette taille prendra très peu de mémoire (ex: ~65 Ko pour du 256x256)
        // et tiendra entièrement dans le cache ultra-rapide du processeur.
        let mut is_chunk = vec![false; width * height];
        let mut is_border = vec![false; width * height];

        // 3. Remplir la grille (Accès mémoire direct et instantané)
        for c in chunks {
            let gx = (c.x - min_x + 1) as usize;
            let gy = (c.y - min_y + 1) as usize;
            is_chunk[gy * width + gx] = true;
        }

        let mut borders = Vec::new();

        // Les offsets pour trouver les 8 voisins dans un tableau 1D
        let w = width as isize;
        let mut neighbor_offsets =
            Vec::with_capacity(((margin * 2 + 1) * (margin * 2 + 1) - 1) as usize);
        for x in -(margin as isize)..=margin as isize {
            for y in -(margin as isize)..=margin as isize {
                if x != 0 || y != 0 {
                    neighbor_offsets.push(x + w * y);
                }
            }
        }

        // 4. Parcourir les chunks et vérifier leurs voisins
        for c in chunks {
            let gx = (c.x - min_x + 1) as usize;
            let gy = (c.y - min_y + 1) as usize;
            let idx = gy * width + gx;

            for &offset in &neighbor_offsets {
                let neighbor_idx = (idx as isize + offset) as usize;

                // Si le voisin n'est pas un chunk, et qu'on ne l'a pas déjà marqué comme bordure
                if !is_chunk[neighbor_idx] && !is_border[neighbor_idx] {
                    is_border[neighbor_idx] = true; // On le marque pour éviter les doublons

                    // Reconvertir l'index 1D en coordonnées x, y
                    let nx = (neighbor_idx % width) as i16 + min_x - 1;
                    let ny = (neighbor_idx / width) as i16 + min_y - 1;

                    borders.push(GameChunk { x: nx, y: ny });
                }
            }
        }

        borders
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Copy, Default, Encode, Decode)]
pub struct GameChunkAera {
    pub x_min: i16,
    pub x_max: i16,
    pub y_min: i16,
    pub y_max: i16,
}

impl From<GameChunk> for GameChunkAera {
    fn from(chunk: GameChunk) -> Self {
        GameChunkAera {
            x_min: chunk.x,
            x_max: chunk.x,
            y_min: chunk.y,
            y_max: chunk.y,
        }
    }
}

impl GameChunkAera {
    pub fn contains(&self, chunk: GameChunk) -> bool {
        chunk.x >= self.x_min
            && chunk.x <= self.x_max
            && chunk.y >= self.y_min
            && chunk.y <= self.y_max
    }
    pub fn iter(&self) -> GameChunkAeraIterator<'_> {
        GameChunkAeraIterator {
            area: self,
            c_x: self.x_min - 1,
            c_y: self.y_min,
        }
    }

    pub fn to_core_rect(&self, size: f32) -> Rect {
        Rect {
            min_x: self.x_min as f32 * size,
            max_x: self.x_max as f32 * size + size,
            min_y: self.y_min as f32 * size,
            max_y: self.y_max as f32 * size + size,
        }
    }

    // donne les chunks de la bordure (1 == les chunks voisins pas dedans, 0 == les chunks les plus sur le bord, -1 == ???)
    pub fn get_borders_as_aera(&self, margin: i8) -> Vec<GameChunkAera> {
        let margin = margin as i16;
        let mut aeras = Vec::with_capacity(4);

        aeras.push(GameChunkAera {
            x_min: self.x_min - margin,
            x_max: self.x_max + margin,
            y_min: self.y_min - margin,
            y_max: self.y_min - margin,
        });

        aeras.push(GameChunkAera {
            x_min: self.x_min - margin,
            x_max: self.x_max + margin,
            y_min: self.y_max + margin,
            y_max: self.y_max + margin,
        });
        aeras.push(GameChunkAera {
            x_min: self.x_min - margin,
            x_max: self.x_min - margin,
            y_min: self.y_min,
            y_max: self.y_max,
        });

        aeras.push(GameChunkAera {
            x_min: self.x_max + margin,
            x_max: self.x_max + margin,
            y_min: self.y_min,
            y_max: self.y_max,
        });

        aeras
    }
    // donne les chunks de la bordure (1 == les chunks voisins pas dedans, 0 == les chunks les plus sur le bord, -1 == ???)
    pub fn get_borders(&self, margin: i8) -> Vec<GameChunk> {
        let mut chunks = Vec::with_capacity(
            ((self.x_max - self.x_min) * 2 + (self.y_max - self.y_min) * 2 + 8) as usize,
        );
        for aeras in self.get_borders_as_aera(margin) {
            for chunk in aeras.iter() {
                chunks.push(chunk);
            }
        }

        chunks
    }
}
pub fn get_chunk_size(world_size: f32, max_division: u8) -> f32 {
    let num_division = 2 << max_division;
    world_size / (num_division as f32)
}

pub struct GameChunkAeraIterator<'a> {
    area: &'a GameChunkAera,
    c_x: i16,
    c_y: i16,
}

impl<'a> Iterator for GameChunkAeraIterator<'a> {
    type Item = GameChunk;
    fn next(&mut self) -> Option<Self::Item> {
        if self.c_x < self.area.x_max {
            self.c_x += 1;
        } else {
            self.c_x = self.area.x_min;
            self.c_y += 1;
        }

        if self.c_y > self.area.y_max {
            None
        } else {
            Some(GameChunk {
                x: self.c_x,
                y: self.c_y,
            })
        }
    }
}
