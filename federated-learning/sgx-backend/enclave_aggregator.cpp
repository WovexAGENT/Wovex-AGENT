// enclave_aggregator.cpp
#include <sgx_urts.h>
#include <sgx_tcrypto.h>
#include <sgx_tseal.h>
#include <sgx_report.h>
#include <sgx_utils.h>
#include <vector>
#include <array>
#include <memory>
#include <stdexcept>
#include <cstring>

#define SAFE_FREE(ptr) { if (ptr) { free(ptr); ptr = nullptr; } }

namespace enclave {
    
    constexpr size_t ENCRYPTED_KEY_SIZE = 256;
    constexpr size_t MAC_SIZE = 16;
    constexpr size_t IV_SIZE = 12;
    constexpr size_t SEALED_DATA_OFFSET = 512;
    constexpr uint16_t SAMPLE_QUOTE_VERSION = 0x0100;

    class EnclaveAggregator {
    public:
        EnclaveAggregator(const char* enclave_path) {
            sgx_launch_token_t token = {0};
            int updated = 0;
            
            auto ret = sgx_create_enclave(
                enclave_path,
                SGX_DEBUG_FLAG,
                &token,
                &updated,
                &enclave_id_,
                nullptr
            );

            if (ret != SGX_SUCCESS) {
                throw std::runtime_error("Enclave creation failed");
            }

            initialize_sealed_storage();
        }

        ~EnclaveAggregator() {
            sgx_destroy_enclave(enclave_id_);
        }

        struct EncryptedData {
            std::vector<uint8_t> ciphertext;
            std::array<uint8_t, IV_SIZE> iv;
            std::array<uint8_t, MAC_SIZE> mac;
        };

        struct AggregateResult {
            std::vector<uint8_t> encrypted_sum;
            std::vector<uint8_t> encrypted_count;
            std::vector<uint8_t> attestation_report;
        };

        AggregateResult process_data(const std::vector<EncryptedData>& inputs) {
            if(inputs.empty()) {
                throw std::invalid_argument("Empty input vector");
            }

            AggregateResult result;
            sgx_status_t ret = SGX_SUCCESS;
            std::vector<uint8_t> sealed_output(SEALED_DATA_OFFSET + inputs.size() * 256);

            ret = ecall_aggregate_data(
                enclave_id_,
                inputs.data(),
                inputs.size(),
                sealed_output.data(),
                sealed_output.size(),
                current_epoch_
            );

            if(ret != SGX_SUCCESS) {
                handle_sgx_error(ret);
            }

            result.encrypted_sum = decrypt_sealed_data(sealed_output);
            result.attestation_report = generate_attestation_report();
            
            rotate_sealing_keys();
            return result;
        }

        std::vector<uint8_t> generate_remote_attestation(
            const std::array<uint8_t, 32>& challenge
        ) {
            sgx_report_data_t report_data = {0};
            sgx_report_t report = {0};
            std::memcpy(report_data.d, challenge.data(), challenge.size());

            auto ret = sgx_create_report(&report_data, &report);
            if(ret != SGX_SUCCESS) {
                handle_sgx_error(ret);
            }

            return std::vector<uint8_t>(
                reinterpret_cast<uint8_t*>(&report),
                reinterpret_cast<uint8_t*>(&report) + sizeof(report)
            );
        }

    private:
        sgx_enclave_id_t enclave_id_;
        uint64_t current_epoch_ = 0;
        std::array<uint8_t, SGX_KEY_SIZE> sealing_key_;
        std::array<uint8_t, SGX_KEY_SIZE> previous_sealing_key_;

        void initialize_sealed_storage() {
            sgx_status_t ret = sgx_get_seal_key(
                SGX_KEYPOLICY_MRENCLAVE,
                &sealing_key_,
                &current_epoch_
            );

            if(ret != SGX_SUCCESS) {
                handle_sgx_error(ret);
            }
        }

        void rotate_sealing_keys() {
            previous_sealing_key_ = sealing_key_;
            
            auto ret = sgx_get_seal_key(
                SGX_KEYPOLICY_MRENCLAVE,
                &sealing_key_,
                &current_epoch_
            );

            if(ret != SGX_SUCCESS) {
                sealing_key_ = previous_sealing_key_;
                handle_sgx_error(ret);
            }
        }

        std::vector<uint8_t> decrypt_sealed_data(const std::vector<uint8_t>& sealed) {
            uint32_t decrypted_size = 0;
            sgx_status_t ret = sgx_unseal_data(
                reinterpret_cast<const sgx_sealed_data_t*>(sealed.data()),
                nullptr,
                &decrypted_size,
                nullptr,
                nullptr
            );

            if(ret != SGX_SUCCESS) {
                handle_sgx_error(ret);
            }

            std::vector<uint8_t> decrypted(decrypted_size);
            ret = sgx_unseal_data(
                reinterpret_cast<const sgx_sealed_data_t*>(sealed.data()),
                nullptr,
                &decrypted_size,
                decrypted.data(),
                nullptr
            );

            if(ret != SGX_SUCCESS) {
                handle_sgx_error(ret);
            }

            return decrypted;
        }

        std::vector<uint8_t> generate_attestation_report() {
            sgx_report_t report;
            sgx_status_t ret = sgx_create_report(nullptr, nullptr, &report);
            
            if(ret != SGX_SUCCESS) {
                handle_sgx_error(ret);
            }

            return std::vector<uint8_t>(
                reinterpret_cast<uint8_t*>(&report),
                reinterpret_cast<uint8_t*>(&report) + sizeof(report)
            );
        }

        void handle_sgx_error(sgx_status_t status) {
            switch(status) {
                case SGX_ERROR_INVALID_PARAMETER:
                    throw std::invalid_argument("Invalid enclave parameter");
                case SGX_ERROR_OUT_OF_MEMORY:
                    throw std::bad_alloc();
                case SGX_ERROR_ENCLAVE_LOST:
                    throw std::runtime_error("Enclave lost during operation");
                case SGX_ERROR_INVALID_ENCLAVE:
                    throw std::runtime_error("Invalid enclave ID");
                default:
                    throw std::runtime_error("SGX operation failed");
            }
        }

        extern "C" {
            sgx_status_t ecall_aggregate_data(
                sgx_enclave_id_t eid,
                const EncryptedData* inputs,
                size_t num_inputs,
                uint8_t* sealed_output,
                size_t sealed_size,
                uint64_t epoch
            );
        }
    };

} // namespace enclave

// Enclave.edl
enclave {
    trusted {
        public sgx_status_t ecall_aggregate_data(
            [in] const EncryptedData* inputs,
            [in] size_t num_inputs,
            [out] uint8_t* sealed_output,
            [in] size_t sealed_size,
            [in] uint64_t epoch
        );

        public sgx_status_t ecall_generate_attestation(
            [out] sgx_report_t* report
        );
    };

    untrusted {
        void ocall_log_message([in] const char* message);
    };
};

// Build instructions
/*
1. Install SGX SDK and PSW
2. Generate signing key:
   sgx_edger8r --trusted enclave.edl
   sgx_sign genkey -enclave enclave.so -out enclave_private.pem
3. Compile with:
   g++ -o enclave_aggregator enclave_aggregator.cpp -lsgx_urts -lsgx_uae_service
4. Run with proper enclave permissions
*/
