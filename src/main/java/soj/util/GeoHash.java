package soj.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.*;
public class GeoHash {

    protected static final byte[] characters = {
            // 0    1    2    3    4    5    6    7
            '0', '1', '2', '3', '4', '5', '6', '7',
            // 8    9    10   11   12   13   14   15
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            // 16   17   18   19   20   21   22   23
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            // 24   25   26   27   28   29   30   31
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
    protected static final byte[] map = new byte['z' + 1];
    static {
        for (byte i = 0; i < characters.length; i++)
            map[characters[i]] = i;
    }

    /** number of bits per character */
    protected static final int      BITS_PER_CHARACTER = 5;
    protected static final int      MAX_BITS = 60;
    protected static final double[] LATITUDE_RANGE = { -90.0, 90.0 };
    protected static final double[] LONGITUDE_RANGE = { -180.0, 180.0 };

    public final double lat, lon;
    /**
     * The precision of the coordinate. Must be in the interval [1-12]
     */
    public final int    precision;
    /**
     * The bit representation of the (LAT,LON)-pair
     */
    public final long   bitValue;
    /**
     * The encoded representation of the coordinate
     */
    public final byte[] hash;

    protected GeoHash(final double lat, final double lon, final long bitValue, final byte[] hash) {
        this.lat = lat;
        this.lon = lon;
        this.precision = hash.length;
        this.bitValue = bitValue;
        this.hash = hash;
    }

    /**
     * @return The encoded geohash
     */
    public final String toHashString() {
        return new String(hash);
    }

    /**
     * @return The binary representation of the coordinate
     */
    public final String toBinaryRepresentation() {
        return new String(binaryRepresentation(bitValue, precision));
    }

    public static final GeoHash decode(final String hash) {
        return decode(hash.getBytes());
    }

    public static final GeoHash decode(final byte[] hash) {
        int lat = 0, lon = 0;// this gives us a bit length of 32 for each coordinate - ought to be sufficient
        boolean evenbit = true;

        // split hash into binary latitude and longitude parts
        long binary = 0;
        for (byte b : hash) {
            b = (byte) (0x1F & map[b]);
            binary <<= BITS_PER_CHARACTER;
            binary |= b;
            // unrolled loop over each bit
            if (evenbit) {
                lon = extractEvenBits(lon, b);
                lat = extractUnevenBits(lat, b);
            } else {
                lat = extractEvenBits(lat, b);
                lon = extractUnevenBits(lon, b);
            }
            evenbit = !evenbit;
        }

        final double latitude  = decodeCoordinate( lat, new GeoHash.Coordinate(0.0, LATITUDE_RANGE, calculateLatitudeBits(hash.length))).coord;
        final double longitude = decodeCoordinate( lon, new GeoHash.Coordinate(0.0, LONGITUDE_RANGE, calculateLongitudeBits(hash.length))).coord;

        return new GeoHash( latitude , longitude, binary, hash);
    }

    protected static final int calculateLatitudeBits(final int precision) {
        return calculateBits(precision, 2);
    }

    protected static final int calculateLongitudeBits(final int precision) {
        return calculateBits(precision, 3);
    }

    /**
     * A mathematical way of calculating the number of bits from input length
     * (length / 2 * 5) + (length % 2 != 0 ? 3 : 0)
     */
    private static final int calculateBits(final int precision, final int unevenExtra) {
        return (((precision >> 1) * BITS_PER_CHARACTER) + ((precision & 0x1) * unevenExtra));
    }

    /**
     * Extracts the even bits, starting with index 1
     * Example:
     * 00010110 = 0x16
     *  | | | |
     *  0 1 1 0 = 0x06
     */
    protected static final int extractEvenBits(int value, final byte b) {
        value <<= 3;
        value |= ((b & 0x10) >> 2);
        value |= ((b & 0x04) >> 1);
        value |= (b & 0x01);
        return value;
    }

    /**
     * Extracts the uneven bits, starting with index 0
     * Example:
     * 00010110 = 0x16
     * | | | |
     * 0 0 0 1 = 0x01
     */
    protected static final int extractUnevenBits(int value, final byte b) {
        value <<= 2;
        value |= ((b & 0x08) >> 2);
        value |= ((b & 0x02) >> 1);
        return value;
    }

    protected static final Coordinate decodeCoordinate(final long bitCoord, final Coordinate coord) {
        double val = 0.0;
        int mask = 1 << coord.bits;
        while ((mask >>= 1) >= 1) {     // while bits are left to be explored
            if ((mask & bitCoord) > 0) {// bit == 1
                coord.min = val;
                val = (val + coord.max) / 2;
            } else {                    // bit == 0
                coord.max = val;
                val = (val + coord.min) / 2;
            }
        }
        // some rounding might be needed
        coord.coord = new BigDecimal(val).setScale(coord.bits / 5, BigDecimal.ROUND_HALF_UP).doubleValue();
        return coord;
    }

    /**
     * Encodes the coordinate-pair into the hash representation
     *
     * @param lat
     *            Latitude coordinate
     * @param lon
     *            Longitude coordinate
     * @param precision
     *            Geohash length must be in the interval [1-12]
     * @return the GeoHash object holding information about the coordinates and
     *         hashed values
     */
    public static final GeoHash encode(final double lat, final double lon, int precision) {
        if (precision < 1) precision = 1;

        final Coordinate latInfo = new Coordinate(lat, LATITUDE_RANGE, calculateLatitudeBits(precision));
        final Coordinate lonInfo = new Coordinate(lon, LONGITUDE_RANGE, calculateLongitudeBits(precision));

        // precision cannot be more than 60 bits (the nearest multiple of 5 under 64 (the bits of a long))
        long mask = 0x1l << Math.min(precision * BITS_PER_CHARACTER, MAX_BITS);
        long bitValue = 0;
        boolean even = true;
        while ((mask >>= 1) > 0) {
            if (even) {
                // longitude
                bitValue = encode(bitValue, lonInfo);
            } else {
                // latitude
                bitValue = encode(bitValue, latInfo);
            }
            even = !even;
        }

        return new GeoHash(lat, lon, bitValue, translateBinaryToHash(bitValue, precision));
    }

    /**
     * See {@link GeoHash#encode(double, double, int)}.
     * This method defaults to the maximal value of precision
     */
    public static final GeoHash encode(final double lat, final double lon) {
        return encode(lat, lon, MAX_BITS / BITS_PER_CHARACTER);
    }

    protected static final long encode(long bitValue, final Coordinate info) {
        info.mid = (info.min + info.max) / 2;
        if (info.coord >= info.mid) {
            bitValue <<= 1;
            bitValue |= 0x1;// add one
            info.min = info.mid;
        } else {
            bitValue <<= 1;// add zero
            info.max = info.mid;
        }
        return bitValue;
    }

    protected static final byte[] translateBinaryToHash(long value, int precision) {
        final byte[] h = new byte[precision];
        while (precision > 0) {
            h[--precision] = characters[(byte) (value & 0x1F)];
            value >>= BITS_PER_CHARACTER;
        }
        return h;
    }

    protected static final String binaryRepresentation(final long bitValue, final int precision) {
        char[] rep = new char[Math.min(precision * BITS_PER_CHARACTER, MAX_BITS)];
        int index = 0;
        long mask = 0x1l << rep.length;
        while ((mask >>= 1) >= 1) // while bits are left to be explored
            rep[index++] = ((mask & bitValue) > 0 ? '1' : '0');

        return new String(rep);
    }

    @Override
    public String toString() {
        return String.format("%f %f %d %s", this.lat, this.lon, this.bitValue, binaryRepresentation(this.bitValue, this.precision));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof GeoHash)
            return Arrays.equals(((GeoHash) obj).hash, this.hash);
        return false;
    }

    @Override
    public int hashCode() {
        return new String(hash).hashCode();
    }

    /**
     * Dataholder for a single coordinate
     */
    protected static final class Coordinate {
        protected double coord, min, max, mid;
        protected int bits;

        public Coordinate(final double coordinate, final double[] range, final int bits) {
            this.coord = coordinate;
            this.min = range[0];
            this.max = range[1];
            this.mid = 0.0;
            this.bits = bits;
        }
    }

    static class A <K,E>{
        private Map<K,E> _map;
        public A(Map<K,E> _map){
            this._map = _map;
        }
        public String toString(){
            StringBuffer sb = new StringBuffer();
            sb.append("Object: \n");
            for(Map.Entry<K, E> e : this._map.entrySet()){
                sb.append(e);
                sb.append("\n");
            }
            return sb.toString();
        }
    }
    public static void main(String []argc){
        System.out.println(GeoHash.encode(30.703971, 104.094640, 7).toHashString());
        PriorityQueue<Pair> pq = new PriorityQueue<Pair>(new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                if((Long)o1.getLeft() > (Long)o2.getLeft()){
                    return -1;
                }
                else if((Long)o1.getLeft() < (Long)o2.getLeft()){
                    return 1;
                }
                return 0;
            }
        });
        pq.add(ImmutablePair.of(102L, "abc"));
        pq.add(ImmutablePair.of(122L, "abc"));
        pq.add(ImmutablePair.of(322L, "abc"));
        while(!pq.isEmpty()){
            System.out.println(pq.poll().getLeft());
        }

        LinkedList<Integer> lst = new LinkedList<Integer>();
        lst.add(1);lst.add(2);lst.add(3);lst.add(5);lst.add(1);lst.add(2);lst.add(3);lst.add(5);
        for(Integer i : lst){
            if(i.equals(1)){
                lst.remove(i);
            }
        }
        for(Integer i : lst){
            System.out.print(i + " ");
        }
    }
}