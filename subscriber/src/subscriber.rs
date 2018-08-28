// Copyright 2018 Bitwise IO
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use sawtooth_sdk::messages::events::{EventSubscription, EventFilter, EventFilter_FilterType, EventList};
use sawtooth_sdk::messages::client_event::{ClientEventsSubscribeRequest, ClientEventsUnsubscribeRequest};
use sawtooth_sdk::messages::validator::Message_MessageType;
use sawtooth_sdk::messaging::zmq_stream::{ZmqMessageConnection, ZmqMessageSender};
use sawtooth_sdk::messaging::stream::{MessageConnection, MessageSender, MessageReceiver};
use bond_common::addressing;
use protobuf;
use event_handler::EventHandler;
use uuid::Uuid;

pub struct Subscriber{
    sender: ZmqMessageSender,
    receiver: MessageReceiver,
    event_handler: EventHandler,
    is_active: bool,
}

impl Subscriber {
    pub fn new(validator_address: &str, event_handler: EventHandler) -> Subscriber {
        let zmq = ZmqMessageConnection::new(validator_address);
        let (mut sender, mut receiver) = zmq.create();
        Subscriber {
            sender,
            receiver,
            event_handler,
            is_active: false
        }
    }

    pub fn start(&mut self, last_known_block_ids: Vec<String>) -> Result<(), String> {
        let event_subscription_request = self.build_subscription_request(last_known_block_ids);
        let content = protobuf::Message::write_to_bytes(&event_subscription_request).map_err(|err| err.to_string())?;
        let correlation_id = Uuid::new_v4().to_string();
        let mut response_future = self.sender.send(Message_MessageType::CLIENT_EVENTS_SUBSCRIBE_REQUEST, &correlation_id, &content).map_err(|err| err.to_string())?;

        self.is_active = true;
        while self.is_active {

            let messaged_received = self.receiver.recv().map_err(|err| err.to_string())?;
            let received = messaged_received.unwrap();
            println!("test {:?}",received.get_message_type() );
            self.event_handler.parse_events(received.get_content())?;

        }
        Ok(())
    }

    fn get_block_commit_subscription(&self) -> EventSubscription {
        let mut block_subscription = EventSubscription::new();
        block_subscription.set_event_type(String::from("sawtooth/block-commit"));
        block_subscription
    }

    fn get_state_delta_subscription(&self) -> EventSubscription {
        let mut state_delta_filter = EventFilter::new();
        state_delta_filter.set_key(String::from("address"));
        state_delta_filter.set_match_string(r"^000000".to_string());//addressing::FAMILY_NAMESPACE));
        state_delta_filter.set_filter_type(EventFilter_FilterType::REGEX_ANY);

        let mut state_delta_subscription = EventSubscription::new();
        state_delta_subscription.set_event_type(String::from("sawtooth/state-delta"));
        state_delta_subscription.set_filters(protobuf::RepeatedField::from_vec(vec![state_delta_filter]));

        state_delta_subscription
    }

    fn build_subscription_request(&self, last_known_block_ids: Vec<String>) -> ClientEventsSubscribeRequest {
        let block_subscription = self.get_block_commit_subscription();
        let state_delta_subscription = self.get_state_delta_subscription();

        let mut event_subscription_request = ClientEventsSubscribeRequest::new();
        event_subscription_request.set_subscriptions(protobuf::RepeatedField::from_vec(vec![block_subscription, state_delta_subscription]));
        event_subscription_request.set_last_known_block_ids(protobuf::RepeatedField::from_vec(last_known_block_ids));

        event_subscription_request
    }

    pub fn stop(&mut self) -> Result<(), String>{
        self.is_active = false;
        let unsusbscribe_request = ClientEventsUnsubscribeRequest::new();
        let content = protobuf::Message::write_to_bytes(&unsusbscribe_request).map_err(|err| err.to_string())?;
        let correlation_id = Uuid::new_v4().to_string();;
        let mut response_future = self.sender.send(Message_MessageType::CLIENT_EVENTS_UNSUBSCRIBE_REQUEST, &correlation_id, &content).map_err(|err| err.to_string())?;
        self.sender.close();
        Ok(())
    }


}
