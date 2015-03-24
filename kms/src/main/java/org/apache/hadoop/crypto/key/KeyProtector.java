package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.KeyRep;
import java.security.Security;
import java.security.Key;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.AlgorithmParameters;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.crypto.Cipher;
import javax.crypto.CipherSpi;
import javax.crypto.SecretKey;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SealedObject;
import javax.crypto.spec.*;

import com.sun.crypto.provider.PBEWithMD5AndTripleDESCipher;

import sun.security.x509.AlgorithmId;
import sun.security.util.DerValue;
import sun.security.util.ObjectIdentifier;

/**
 * This class implements a protection mechanism for private keys. In JCE, we
 * use a stronger protection mechanism than in the JDK, because we can use
 * the <code>Cipher</code> class.
 * Private keys are protected using the JCE mechanism, and are recovered using
 * either the JDK or JCE mechanism, depending on how the key has been
 * protected. This allows us to parse Sun's keystore implementation that ships
 * with JDK 1.2.
 *
 * @author Jan Luehe
 *
 *
 * @see JceKeyStore
 */

final class KeyProtector {

    // defined by SunSoft (SKI project)
    private static final String PBE_WITH_MD5_AND_DES3_CBC_OID
            = "1.3.6.1.4.1.42.2.19.1";

    // JavaSoft proprietary key-protection algorithm (used to protect private
    // keys in the keystore implementation that comes with JDK 1.2)
    private static final String KEY_PROTECTOR_OID = "1.3.6.1.4.1.42.2.17.1.1";

    private static final int SALT_LEN = 20; // the salt length
    private static final int DIGEST_LEN = 20;

    // the password used for protecting/recovering keys passed through this
    // key protector
    private char[] password;

    private static final Provider PROV = Security.getProvider("SunJCE");
    
    KeyProtector(char[] password) {
        if (password == null) {
           throw new IllegalArgumentException("password can't be null");
        }
        this.password = password;
    }

    /**
     * Protects the given cleartext private key, using the password provided at
     * construction time.
     */
    byte[] protect(PrivateKey key)
        throws Exception
    {
        // create a random salt (8 bytes)
        byte[] salt = new byte[8];
        new SecureRandom().nextBytes(salt);

        // create PBE parameters from salt and iteration count
        PBEParameterSpec pbeSpec = new PBEParameterSpec(salt, 20);

        // create PBE key from password
        PBEKeySpec pbeKeySpec = new PBEKeySpec(this.password);
        SecretKey sKey = new PBEKey(pbeKeySpec, "PBEWithMD5AndTripleDES");
        pbeKeySpec.clearPassword();

        // encrypt private key
        Cipher cipher = Cipher.getInstance("PBEWithMD5AndTripleDES");
        cipher.init(Cipher.ENCRYPT_MODE, sKey, pbeSpec, null);
        byte[] plain = (byte[])key.getEncoded();
        byte[] encrKey = cipher.doFinal(plain, 0, plain.length);
        
        // wrap encrypted private key in EncryptedPrivateKeyInfo
        // (as defined in PKCS#8)
        AlgorithmParameters pbeParams = AlgorithmParameters.getInstance("PBE", PROV);
        pbeParams.init(pbeSpec);

        AlgorithmId encrAlg = new AlgorithmId(new ObjectIdentifier(PBE_WITH_MD5_AND_DES3_CBC_OID), pbeParams);
        return new EncryptedPrivateKeyInfo(encrAlg,encrKey).getEncoded();
    }

    /*
     * Recovers the cleartext version of the given key (in protected format),
     * using the password provided at construction time.
     */
    Key recover(EncryptedPrivateKeyInfo encrInfo)
        throws UnrecoverableKeyException, NoSuchAlgorithmException
    {
        byte[] plain;

        try {
            String encrAlg = encrInfo.getAlgorithm().getOID().toString();
            if (!encrAlg.equals(PBE_WITH_MD5_AND_DES3_CBC_OID)
                && !encrAlg.equals(KEY_PROTECTOR_OID)) {
                throw new UnrecoverableKeyException("Unsupported encryption "
                                                    + "algorithm");
            }

            if (encrAlg.equals(KEY_PROTECTOR_OID)) {
                // JDK 1.2 style recovery
                plain = recover(encrInfo.getEncryptedData());
            } else {
                byte[] encodedParams =
                    encrInfo.getAlgorithm().getEncodedParams();

                // parse the PBE parameters into the corresponding spec
                AlgorithmParameters pbeParams =
                    AlgorithmParameters.getInstance("PBE");
                pbeParams.init(encodedParams);
                PBEParameterSpec pbeSpec = (PBEParameterSpec)
                    pbeParams.getParameterSpec(PBEParameterSpec.class);

                // create PBE key from password
                PBEKeySpec pbeKeySpec = new PBEKeySpec(this.password);
                SecretKey sKey =
                    new PBEKey(pbeKeySpec, "PBEWithMD5AndTripleDES");
                pbeKeySpec.clearPassword();

                // decrypt private key
                Cipher cipher = Cipher.getInstance("PBEWithMD5AndTripleDES");
                cipher.init(Cipher.DECRYPT_MODE, sKey, pbeSpec, null);
                plain= cipher.doFinal(encrInfo.getEncryptedData(), 0, encrInfo.getEncryptedData().length);
            }

            // determine the private-key algorithm, and parse private key
            // using the appropriate key factory
            String oidName = new AlgorithmId(new PrivateKeyInfo(plain).getAlgorithm().getOID()).getName();
            KeyFactory kFac = KeyFactory.getInstance(oidName);
            return kFac.generatePrivate(new PKCS8EncodedKeySpec(plain));

        } catch (NoSuchAlgorithmException ex) {
            // Note: this catch needed to be here because of the
            // later catch of GeneralSecurityException
            throw ex;
        } catch (IOException ioe) {
            throw new UnrecoverableKeyException(ioe.getMessage());
        } catch (GeneralSecurityException gse) {
            throw new UnrecoverableKeyException(gse.getMessage());
        }
    }

    /*
     * Recovers the cleartext version of the given key (in protected format),
     * using the password provided at construction time. This method implements
     * the recovery algorithm used by Sun's keystore implementation in
     * JDK 1.2.
     */
    private byte[] recover(byte[] protectedKey)
        throws UnrecoverableKeyException, NoSuchAlgorithmException
    {
        int i, j;
        byte[] digest;
        int numRounds;
        int xorOffset; // offset in xorKey where next digest will be stored
        int encrKeyLen; // the length of the encrpyted key

        MessageDigest md = MessageDigest.getInstance("SHA");

        // Get the salt associated with this key (the first SALT_LEN bytes of
        // <code>protectedKey</code>)
        byte[] salt = new byte[SALT_LEN];
        System.arraycopy(protectedKey, 0, salt, 0, SALT_LEN);

        // Determine the number of digest rounds
        encrKeyLen = protectedKey.length - SALT_LEN - DIGEST_LEN;
        numRounds = encrKeyLen / DIGEST_LEN;
        if ((encrKeyLen % DIGEST_LEN) != 0)
            numRounds++;

        // Get the encrypted key portion and store it in "encrKey"
        byte[] encrKey = new byte[encrKeyLen];
        System.arraycopy(protectedKey, SALT_LEN, encrKey, 0, encrKeyLen);

        // Set up the byte array which will be XORed with "encrKey"
        byte[] xorKey = new byte[encrKey.length];

        // Convert password to byte array, so that it can be digested
        byte[] passwdBytes = new byte[password.length * 2];
        for (i=0, j=0; i<password.length; i++) {
            passwdBytes[j++] = (byte)(password[i] >> 8);
            passwdBytes[j++] = (byte)password[i];
        }

        // Compute the digests, and store them in "xorKey"
        for (i = 0, xorOffset = 0, digest = salt;
             i < numRounds;
             i++, xorOffset += DIGEST_LEN) {
            md.update(passwdBytes);
            md.update(digest);
            digest = md.digest();
            md.reset();
            // Copy the digest into "xorKey"
            if (i < numRounds - 1) {
                System.arraycopy(digest, 0, xorKey, xorOffset,
                                 digest.length);
            } else {
                System.arraycopy(digest, 0, xorKey, xorOffset,
                                 xorKey.length - xorOffset);
            }
        }

        // XOR "encrKey" with "xorKey", and store the result in "plainKey"
        byte[] plainKey = new byte[encrKey.length];
        for (i = 0; i < plainKey.length; i++) {
            plainKey[i] = (byte)(encrKey[i] ^ xorKey[i]);
        }

        // Check the integrity of the recovered key by concatenating it with
        // the password, digesting the concatenation, and comparing the
        // result of the digest operation with the digest provided at the end
        // of <code>protectedKey</code>. If the two digest values are
        // different, throw an exception.
        md.update(passwdBytes);
        java.util.Arrays.fill(passwdBytes, (byte)0x00);
        passwdBytes = null;
        md.update(plainKey);
        digest = md.digest();
        md.reset();
        for (i = 0; i < digest.length; i++) {
            if (digest[i] != protectedKey[SALT_LEN + encrKeyLen + i]) {
                throw new UnrecoverableKeyException("Cannot recover key");
            }
        }
        return plainKey;
    }

    /**
     * Seals the given cleartext key, using the password provided at
     * construction time
     */
    SealedObject seal(Key key)
        throws Exception
    {
        // create a random salt (8 bytes)
        byte[] salt = new byte[8];
        new SecureRandom().nextBytes(salt);

        // create PBE parameters from salt and iteration count
        PBEParameterSpec pbeSpec = new PBEParameterSpec(salt, 20);

        // create PBE key from password
        PBEKeySpec pbeKeySpec = new PBEKeySpec(this.password);
        SecretKey sKey = new PBEKey(pbeKeySpec, "PBEWithMD5AndTripleDES");
        pbeKeySpec.clearPassword();

        // seal key
        Cipher cipher;

        PBEWithMD5AndTripleDESCipher cipherSpi;
        cipherSpi = new PBEWithMD5AndTripleDESCipher();
        cipher = new CipherForKeyProtector(cipherSpi, PROV,
                                           "PBEWithMD5AndTripleDES");
        cipher.init(Cipher.ENCRYPT_MODE, sKey, pbeSpec);
        return new SealedObjectForKeyProtector(key, cipher);
    }

    /**
     * Unseals the sealed key.
     */
    Key unseal(SealedObject so)
        throws NoSuchAlgorithmException, UnrecoverableKeyException
    {
        try {
            // create PBE key from password
            PBEKeySpec pbeKeySpec = new PBEKeySpec(this.password);
            SecretKey skey = new PBEKey(pbeKeySpec, "PBEWithMD5AndTripleDES");
            pbeKeySpec.clearPassword();

            SealedObjectForKeyProtector soForKeyProtector = null;
            if (!(so instanceof SealedObjectForKeyProtector)) {
                soForKeyProtector = new SealedObjectForKeyProtector(so);
            } else {
                soForKeyProtector = (SealedObjectForKeyProtector)so;
            }
            AlgorithmParameters params = soForKeyProtector.getParameters();
            if (params == null) {
                throw new UnrecoverableKeyException("Cannot get " +
                                                    "algorithm parameters");
            }
            PBEWithMD5AndTripleDESCipher cipherSpi;
            cipherSpi = new PBEWithMD5AndTripleDESCipher();
            Cipher cipher = new CipherForKeyProtector(cipherSpi, PROV,
                                                   "PBEWithMD5AndTripleDES");
            cipher.init(Cipher.DECRYPT_MODE, skey, params);
            return (Key)soForKeyProtector.getObject(cipher);
        } catch (NoSuchAlgorithmException ex) {
            // Note: this catch needed to be here because of the
            // later catch of GeneralSecurityException
            throw ex;
        } catch (IOException ioe) {
            throw new UnrecoverableKeyException(ioe.getMessage());
        } catch (ClassNotFoundException cnfe) {
            throw new UnrecoverableKeyException(cnfe.getMessage());
        } catch (GeneralSecurityException gse) {
            throw new UnrecoverableKeyException(gse.getMessage());
        }
    }
}


final class CipherForKeyProtector extends javax.crypto.Cipher {
    /**
     * Creates a Cipher object.
     *
     * @param cipherSpi the delegate
     * @param provider the provider
     * @param transformation the transformation
     */
    protected CipherForKeyProtector(CipherSpi cipherSpi,
                                    Provider provider,
                                    String transformation) {
        super(cipherSpi, provider, transformation);
    }
}

final class SealedObjectForKeyProtector extends javax.crypto.SealedObject {

    static final long serialVersionUID = -3650226485480866989L;

    SealedObjectForKeyProtector(Serializable object, Cipher c)
        throws IOException, IllegalBlockSizeException {
        super(object, c);
    }

    SealedObjectForKeyProtector(SealedObject so) {
        super(so);
    }

    AlgorithmParameters getParameters() {
        AlgorithmParameters params = null;
        if (super.encodedParams != null) {
            try {
                params = AlgorithmParameters.getInstance("PBE", "SunJCE");
                params.init(super.encodedParams);
            } catch (NoSuchProviderException nspe) {
                // eat.
            } catch (NoSuchAlgorithmException nsae) {
                //eat.
            } catch (IOException ioe) {
                //eat.
            }
        }
        return params;
    }
}

final class PrivateKeyInfo {

    // the version number defined by the PKCS #8 standard
    private static final BigInteger VERSION = BigInteger.ZERO;

    // the private-key algorithm
    private AlgorithmId algid;

    // the private-key value
    private byte[] privkey;

    /**
     * Constructs a PKCS#8 PrivateKeyInfo from its ASN.1 encoding.
     */
    PrivateKeyInfo(byte[] encoded) throws IOException {
        DerValue val = new DerValue(encoded);

        if (val.tag != DerValue.tag_Sequence)
            throw new IOException("private key parse error: not a sequence");

        // version
        BigInteger parsedVersion = val.data.getBigInteger();
        if (!parsedVersion.equals(VERSION)) {
            throw new IOException("version mismatch: (supported: " +
                                  VERSION + ", parsed: " + parsedVersion);
        }

        // privateKeyAlgorithm
        this.algid = AlgorithmId.parse(val.data.getDerValue());

        // privateKey
        this.privkey = val.data.getOctetString();

        // OPTIONAL attributes not supported yet
    }

    /**
     * Returns the private-key algorithm.
     */
    AlgorithmId getAlgorithm() {
        return this.algid;
    }
}

/**
 * This class represents a PBE key.
 *
 * @author Jan Luehe
 *
 */
final class PBEKey implements SecretKey {

    static final long serialVersionUID = -2234768909660948176L;

    private byte[] key;

    private String type;

    /**
     * Creates a PBE key from a given PBE key specification.
     *
     * @param key the given PBE key specification
     */
    PBEKey(PBEKeySpec keySpec, String keytype) throws InvalidKeySpecException {
        char[] passwd = keySpec.getPassword();
        if (passwd == null) {
            // Should allow an empty password.
            passwd = new char[0];
        }
        for (int i=0; i<passwd.length; i++) {
            if ((passwd[i] < '\u0020') || (passwd[i] > '\u007E')) {
                throw new InvalidKeySpecException("Password is not ASCII");
            }
        }
        this.key = new byte[passwd.length];
        for (int i=0; i<passwd.length; i++)
            this.key[i] = (byte) (passwd[i] & 0x7f);
        java.util.Arrays.fill(passwd, ' ');
        type = keytype;
    }

    public byte[] getEncoded() {
        return (byte[])this.key.clone();
    }

    public String getAlgorithm() {
        return type;
    }

    public String getFormat() {
        return "RAW";
    }

    /**
     * Calculates a hash code value for the object.
     * Objects that are equal will also have the same hashcode.
     */
    public int hashCode() {
        int retval = 0;
        for (int i = 1; i < this.key.length; i++) {
            retval += this.key[i] * i;
        }
        return(retval ^= getAlgorithm().toLowerCase().hashCode());
    }

    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof SecretKey))
            return false;

        SecretKey that = (SecretKey)obj;

        if (!(that.getAlgorithm().equalsIgnoreCase(type)))
            return false;

        byte[] thatEncoded = that.getEncoded();
        boolean ret = java.util.Arrays.equals(this.key, thatEncoded);
        java.util.Arrays.fill(thatEncoded, (byte)0x00);
        return ret;
    }

    /**
     * readObject is called to restore the state of this key from
     * a stream.
     */
    private void readObject(java.io.ObjectInputStream s)
         throws java.io.IOException, ClassNotFoundException
    {
        s.defaultReadObject();
        key = (byte[])key.clone();
    }


    /**
     * Replace the PBE key to be serialized.
     *
     * @return the standard KeyRep object to be serialized
     *
     * @throws java.io.ObjectStreamException if a new object representing
     * this PBE key could not be created
     */
    private Object writeReplace() throws java.io.ObjectStreamException {
        return new KeyRep(KeyRep.Type.SECRET,
                        getAlgorithm(),
                        getFormat(),
                        getEncoded());
    }

    /**
     * Ensures that the password bytes of this key are
     * set to zero when there are no more references to it.
     */
    protected void finalize() throws Throwable {
        try {
            if (this.key != null) {
                java.util.Arrays.fill(this.key, (byte)0x00);
                this.key = null;
            }
        } finally {
            super.finalize();
        }
    }
}


