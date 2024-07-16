export function decode(string) {
  try {
    const result = JSON.parse(string);
    return new Ok(result);
  } catch (err) {
    return new Error(`Failed to parse JSON: ${err}`);
  }
}
