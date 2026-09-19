//! Minimal XML helpers shared by the REST providers' LIST and error parsing.

/// Values of every `<tag>…</tag>` element in `body`, entity-decoded.
pub(crate) fn extract_xml_tag_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after_open = &rest[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            break;
        };
        values.push(decode_xml_entities(&after_open[..end]));
        rest = &after_open[end + close.len()..];
    }
    values
}

/// Decode XML character references in one left-to-right pass.
///
/// Handles the five predefined entities and decimal or hex numeric
/// references. Decoding in a single pass keeps `&amp;lt;` as the literal
/// text `&lt;` instead of decoding it twice. Anything else is kept
/// literally; well-formed provider XML cannot contain it.
pub(crate) fn decode_xml_entities(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(amp) = rest.find('&') {
        decoded.push_str(&rest[..amp]);
        let candidate = &rest[amp..];
        let replacement = candidate
            .find(';')
            .and_then(|semi| decode_reference(&candidate[1..semi]).map(|ch| (ch, semi)));
        if let Some((ch, semi)) = replacement {
            decoded.push(ch);
            rest = &candidate[semi + 1..];
        } else {
            decoded.push('&');
            rest = &candidate[1..];
        }
    }
    decoded.push_str(rest);
    decoded
}

fn decode_reference(name: &str) -> Option<char> {
    match name {
        "amp" => Some('&'),
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        _ => {
            let digits = name.strip_prefix('#')?;
            let code = match digits.strip_prefix(['x', 'X']) {
                Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                None => digits.parse::<u32>().ok()?,
            };
            char::from_u32(code)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_escaped_ampersand_once_when_listing_keys() {
        // Arrange: `a&amp;lt;b` is the correct encoding of the key `a&lt;b`.
        let body = "<ListBucketResult><Key>p/a&amp;lt;b</Key></ListBucketResult>";

        // Act
        let keys = extract_xml_tag_values(body, "Key");

        // Assert
        assert_eq!(keys, vec!["p/a&lt;b".to_string()]);
    }

    #[test]
    fn should_decode_numeric_character_reference_when_listing_keys() {
        // Arrange: S3 encodes control characters as numeric references.
        let body = "<Key>line&#13;break&#x0A;end</Key>";

        // Act
        let keys = extract_xml_tag_values(body, "Key");

        // Assert
        assert_eq!(keys, vec!["line\rbreak\nend".to_string()]);
    }

    #[test]
    fn should_keep_unrecognized_ampersand_text_literally_when_decoding() {
        // Arrange
        let value = "a & b &unknown; &#xZZ; &amp;";

        // Act
        let decoded = decode_xml_entities(value);

        // Assert
        assert_eq!(decoded, "a & b &unknown; &#xZZ; &");
    }
}
