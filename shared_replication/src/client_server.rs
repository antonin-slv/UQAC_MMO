// -- les différents streams de données

use rocket::serde::{Deserialize, Serialize};

//
// CLIENT SERVER COMMUNICATION
//
#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct EntitySnapshot {
    pub network_id: u32,
    pub position: [f32; 2],
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersonalSnapshot {
    pub entities: Vec<EntitySnapshot>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct PlayerInput(pub u32);

impl PlayerInput {
    pub fn get_bit(&self, pos: u8) -> bool {
        (self.0 >> pos) & 1 == 1
    }
    pub fn is_up(&self) -> bool {
        self.get_bit(0)
    }
    pub fn is_down(&self) -> bool {
        self.get_bit(1)
    }
    pub fn is_left(&self) -> bool {
        self.get_bit(2)
    }

    pub fn is_right(&self) -> bool {
        self.get_bit(3)
    }

    //set pos-th bit to bit value. (0 being right)
    pub fn set_bit(&mut self, bit: bool, pos: u8) {
        self.0 = (self.0 & !(1u32 << pos)) | ((bit as u32) << pos);
    }

    pub fn set_up(&mut self, up: bool) {
        self.set_bit(up, 0);
    }

    pub fn set_down(&mut self, down: bool) {
        self.set_bit(down, 1);
    }
    pub fn set_left(&mut self, left: bool) {
        self.set_bit(left, 2);
    }

    pub fn set_right(&mut self, right: bool) {
        self.set_bit(right, 3);
    }
}
