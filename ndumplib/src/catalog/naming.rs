pub fn decompress_rom_name(rom_name: &str, game_name: &str) -> String {
    if rom_name == "$c" {
        format!("{}.cue", game_name)
    } else if rom_name == "$i" {
        format!("{}.iso", game_name)
    } else if rom_name == "$b" {
        format!("{}.bin", game_name)
    } else if rom_name.starts_with("$T") {
        format!("{} (Track {}).bin", game_name, rom_name[2..].to_string())
    } else {
        rom_name.replace("#", game_name)
    }
}

pub fn compress_rom_name(rom_name: &str, game_name: &str) -> String {
    let first_step = rom_name.replace(game_name, "#");
    if first_step.starts_with("# (Track ") && first_step.ends_with(").bin") {
        return format!("$T{}", first_step[9..(first_step.len() - 5)].to_string());
    } else if first_step == "#.cue" {
        return String::from("$c");
    } else if first_step == "#.iso" {
        return String::from("$i");
    } else if first_step == "#.bin" {
        return String::from("$b");
    }
    first_step
}
