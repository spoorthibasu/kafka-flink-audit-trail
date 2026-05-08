package com.example.audit;

// Sparse event: only the fields that changed on a given event are set.
// eventSequence is a monotonic counter per customer, assigned at the producer,
// used as a tiebreaker when two events share the same timestamp.
public class CustomerProfileChangeEvent {

    private String customerId;
    private long eventTimestamp;
    private long eventSequence;
    private String email;
    private String phone;
    private String address;

    public CustomerProfileChangeEvent() {}

    public CustomerProfileChangeEvent(
            String customerId,
            long eventTimestamp,
            long eventSequence,
            String email,
            String phone,
            String address) {
        this.customerId = customerId;
        this.eventTimestamp = eventTimestamp;
        this.eventSequence = eventSequence;
        this.email = email;
        this.phone = phone;
        this.address = address;
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public long getEventSequence() { return eventSequence; }
    public void setEventSequence(long eventSequence) { this.eventSequence = eventSequence; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
}
