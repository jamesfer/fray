// Generation can be updated when:
// - an upstream operator moves to another node, therefore changing its address
// - autoscaling causing each operator to need to use a different set of partitions

pub struct GenerationInputLocation {
    offset_range: (usize, usize),
    partitions: Vec<usize>,
}

pub struct GenerationInputDetail {
    stream_id: String,
    locations: Vec<GenerationInputLocation>,
}

pub enum GenerationStartOffset {
    AnyTimeAfter(usize),
    Exactly(usize),
}

pub struct GenerationSpec {
    id: String,
    partitions: Vec<usize>,
    starting_at: Vec<(String, GenerationStartOffset)>,
    input_details: Vec<GenerationInputDetail>,
}