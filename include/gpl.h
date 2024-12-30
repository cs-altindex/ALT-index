#ifndef ALT_INDEX_GPL_H
#define ALT_INDEX_GPL_H

//#define ENABLE_DEBUG

namespace alt_index
{

    // GPL algorithm parameters
    struct Segment
    {
        uint64_t firstKey;  // First key of this segment
        int numItems;      // Number of items in the segment
        double upperSlope; // Upper slope of the segment
        double lowerSlope; // Lower slope of the segment
    };

    /**
     * @brief Perform segment partitioning using the GPL algorithm
     * @param key_arr Array of keys
     * @param key_nums Number of data points
     * @param ret Reference to the GPL segment result
     * @param epsilon Error bound for the GPL algorithm
     */
    template <class KeyType>
    void segmentPartition(KeyType *key_arr, int key_nums, Segment &ret, int epsilon)
    {
#ifdef ENABLE_DEBUG
        std::cout << "Starting segment partitioning with " << key_nums << " keys and epsilon " << epsilon << std::endl;
#endif

        if (key_nums == 1)
        {
            ret.firstKey = key_arr[0];
            ret.numItems = 1;
            ret.upperSlope = 0.0;
            ret.lowerSlope = 0.0;
#ifdef ENABLE_DEBUG
            std::cout << "Segment partitioned with 1 key: firstKey=" << ret.firstKey 
                      << ", numItems=" << ret.numItems 
                      << ", upperSlope=" << ret.upperSlope 
                      << ", lowerSlope=" << ret.lowerSlope << std::endl;
#endif
            return;
        }

        if (key_nums == 2)
        {
            ret.firstKey = key_arr[0];
            ret.numItems = 2;
            ret.upperSlope = ret.lowerSlope = 1.0 / (key_arr[1] - key_arr[0]);
#ifdef ENABLE_DEBUG
            std::cout << "Segment partitioned with 2 keys: firstKey=" << ret.firstKey 
                      << ", numItems=" << ret.numItems 
                      << ", upperSlope=" << ret.upperSlope 
                      << ", lowerSlope=" << ret.lowerSlope << std::endl;
#endif
            return;
        }

        double error1 = 0; // Record first function error
        double error2 = 0; // Record second function error
        double upperSlope = 1.0 / (key_arr[1] - key_arr[0]);
        double lowerSlope = upperSlope;
        int cur_pos = 2;

        double new_slope = (cur_pos - 0.0) / (key_arr[cur_pos] - key_arr[0]);

        error1 = upperSlope * (key_arr[cur_pos] - key_arr[0]) - cur_pos;
        error2 = cur_pos - lowerSlope * (key_arr[cur_pos] - key_arr[0]);

#ifdef ENABLE_DEBUG
        std::cout << "Initial slopes: upperSlope=" << upperSlope << ", lowerSlope=" << lowerSlope 
                  << ", new_slope=" << new_slope 
                  << ", error1=" << error1 << ", error2=" << error2 << std::endl;
#endif

        // My segment partition algorithm
        while (cur_pos < key_nums)
        {
#ifdef ENABLE_DEBUG
            std::cout << "Current position: " << cur_pos 
                      << ", upperSlope=" << upperSlope 
                      << ", lowerSlope=" << lowerSlope 
                      << ", new_slope=" << new_slope 
                      << ", error1=" << error1 << ", error2=" << error2 << std::endl;
#endif

            if (std::max(error1, error2) > epsilon)
            {
#ifdef ENABLE_DEBUG
                std::cout << "Error exceeds epsilon, breaking loop." << std::endl;
#endif
                break;
            }
            cur_pos++;

            if (upperSlope < new_slope)
            {
                upperSlope = new_slope;
#ifdef ENABLE_DEBUG
                std::cout << "Updated upperSlope to " << upperSlope << std::endl;
#endif
            }
            if (lowerSlope > new_slope)
            {
                lowerSlope = new_slope;
#ifdef ENABLE_DEBUG
                std::cout << "Updated lowerSlope to " << lowerSlope << std::endl;
#endif
            }

            new_slope = (cur_pos - 0.0) / (key_arr[cur_pos] - key_arr[0]);
#ifdef ENABLE_DEBUG
            std::cout << "Updated new_slope to " << new_slope << std::endl;
#endif

            error1 = upperSlope * (key_arr[cur_pos] - key_arr[0]) - cur_pos;
            error2 = cur_pos - lowerSlope * (key_arr[cur_pos] - key_arr[0]);
#ifdef ENABLE_DEBUG
            std::cout << "Updated errors: error1=" << error1 << ", error2=" << error2 << std::endl;
#endif
        }

        // Set the results of the segment partition
        ret.firstKey = key_arr[0];
        ret.numItems = cur_pos;
        ret.upperSlope = upperSlope;
        ret.lowerSlope = lowerSlope;
#ifdef ENABLE_DEBUG
        std::cout << "Final segment partition result: firstKey=" << ret.firstKey 
                  << ", numItems=" << ret.numItems 
                  << ", upperSlope=" << ret.upperSlope 
                  << ", lowerSlope=" << ret.lowerSlope << std::endl;
#endif
        return;
    }
}

#endif // ALT_INDEX_GPL_H