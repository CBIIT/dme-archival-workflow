package gov.nih.nci.hpc.dmesync.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

/**
 * Unit tests for {@link DocQueueNameResolver}.
 */
class DocQueueNameResolverTest {

  private final DocQueueNameResolver resolver = new DocQueueNameResolver();

  // ── resolve(String) ────────────────────────────────────────────────────────

  @Test
  void nullDocName_returnsFallback() {
    assertEquals(DocQueueNameResolver.FALLBACK, resolver.resolve((String) null));
  }

  @Test
  void blankDocName_returnsFallback() {
    assertEquals(DocQueueNameResolver.FALLBACK, resolver.resolve("   "));
  }

  @Test
  void emptyDocName_returnsFallback() {
    assertEquals(DocQueueNameResolver.FALLBACK, resolver.resolve(""));
  }

  @Test
  void specialCharsOnly_returnsFallback() {
    assertEquals(DocQueueNameResolver.FALLBACK, resolver.resolve("---"));
  }

  @Test
  void simpleUpperCase_normalizedToLower() {
    assertEquals("dme.doc1", resolver.resolve("DOC1"));
  }

  @Test
  void spaceSeparated_replacedWithUnderscore() {
    assertEquals("dme.my_doc", resolver.resolve("My DOC"));
  }

  @Test
  void hyphenSlash_replacedWithSingleUnderscore() {
    assertEquals("dme.doc_a_b", resolver.resolve("DOC-A/B"));
  }

  @Test
  void leadingAndTrailingSpecialChars_stripped() {
    assertEquals("dme.leading", resolver.resolve("_leading_"));
  }

  @Test
  void digitsOnly_preserved() {
    assertEquals("dme.123", resolver.resolve("123"));
  }

  @Test
  void mixedCase_normalizedToLower() {
    assertEquals("dme.cclc", resolver.resolve("CCLC"));
  }

  @Test
  void multipleConsecutiveSpecialChars_collapsedToOneUnderscore() {
    assertEquals("dme.a_b", resolver.resolve("A  --  B"));
  }

  // ── resolve(DocConfig) ─────────────────────────────────────────────────────

  @Test
  void nullConfig_returnsFallback() {
    assertEquals(DocQueueNameResolver.FALLBACK, resolver.resolve((DocConfig) null));
  }

  @Test
  void configWithDocName_resolvesCorrectly() {
    DocConfig config = new DocConfig(
        1L, "My Lab", null, "My Lab", null, null, null,
        true, null, 0, null, null,
        null, null, null, null, null, null);
    assertEquals("dme.my_lab", resolver.resolve(config));
  }
}
