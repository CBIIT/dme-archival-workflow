package gov.nih.nci.hpc.dmesync.jms;

import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

/**
 * Derives a per-DOC JMS queue name from an Oracle DOC definition.
 *
 * <p>Queue name format: {@code dme.<normalized-doc-name>}
 *
 * <p>Normalization rules applied to the raw DOC name:
 * <ol>
 *   <li>Trim surrounding whitespace.</li>
 *   <li>Convert to lower-case.</li>
 *   <li>Replace every run of non-alphanumeric characters with {@code _}.</li>
 *   <li>Strip any leading or trailing {@code _} characters.</li>
 * </ol>
 *
 * <p>If the DOC name is {@code null}, blank, or collapses to an empty string after
 * normalization, the fallback queue name {@value #FALLBACK} is returned so that no
 * message is ever silently dropped.
 *
 * <p>Examples:
 * <pre>
 *   "DOC1"       → "dme.doc1"
 *   "My DOC"     → "dme.my_doc"
 *   "DOC-A/B"    → "dme.doc_a_b"
 *   "_leading_"  → "dme.leading"
 *   null         → "dme.default"
 * </pre>
 */
@Component
public class DocQueueNameResolver {

  /** Prefix applied to every resolved queue name. */
  public static final String PREFIX = "dme.";

  /**
   * Fallback queue name used when a DOC name is blank or cannot be normalized to a
   * non-empty string.
   */
  public static final String FALLBACK = "dme.default";

  /**
   * Resolves a queue name from a raw DOC name string.
   *
   * @param docName the Oracle DOC name (may be {@code null} or blank)
   * @return the resolved queue name, never {@code null}
   */
  public String resolve(String docName) {
    if (docName == null || docName.isBlank()) {
      return FALLBACK;
    }
    String normalized = docName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_");
    // Strip any leading or trailing underscores produced by the substitution.
    normalized = normalized.replaceAll("^_+|_+$", "");
    if (normalized.isEmpty()) {
      return FALLBACK;
    }
    return PREFIX + normalized;
  }

  /**
   * Resolves a queue name from a {@link DocConfig}.
   *
   * @param config the DOC configuration (may be {@code null})
   * @return the resolved queue name, never {@code null}
   */
  public String resolve(DocConfig config) {
    if (config == null) {
      return FALLBACK;
    }
    return resolve(config.getDocName());
  }
}
